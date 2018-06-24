# -*- coding: utf-8 -*-

__all__ = ['ProcessPool', 'ProcessPoolException']

import time
import logging
import Queue
import traceback
from threading import Thread
from multiprocessing import (
    JoinableQueue,
    Process,
    Value,
    cpu_count,
)


STOPPED = 0
RUNNING = 1


class ProcessPoolException(Exception):
    def __init__(self, message=None):
        super(ProcessPoolException, self).__init__(message)


class Worker(Process):

    def __init__(self, pool, logger_name=None):
        super(Worker, self).__init__()
        self.logger = logging.getLogger(logger_name or __name__)
        self.pool = pool
        self.interval = pool.interval
        self._state = Value('d', STOPPED)

    def run(self):
        while self.is_active():
            try:
                (work_class, args, kwargs) = self.get_nowait()
                try:
                    result = work_class(*args, **kwargs)
                    self.register_result(result)
                except Exception as exc:
                    self.trace_exc(exc, traceback.format_exc())
                finally:
                    self.task_done()
                    if self.interval:
                        time.sleep(self.interval)
            except Queue.Empty:
                time.sleep(0.1)
                continue
            except (EOFError, OSError) as exc:
                self.trace_exc(
                    exc,
                    traceback.format_exc())
                self.stop()

    def is_active(self):
        return self.state == RUNNING

    def get_nowait(self):
        return self.pool.task_queue.get_nowait()

    def trace_exc(self, exc, tb=None):
        try:
            self.pool.exc_queue.put((exc, tb), True, 1)
        except Exception:
            pass

    def register_result(self, result):
        try:
            self.pool.result_queue.put(result, True, 1)
        except Exception:
            pass

    def task_done(self):
        self.pool.task_queue.task_done()

    @property
    def state(self):
        return self._state.value

    @state.setter
    def state(self, val):
        self._state.value = val

    def start(self):
        self.state = RUNNING
        super(Worker, self).start()

    def stop(self):
        self.state = STOPPED


class SentinelThread(Thread):

    def __init__(self, pool, logger_name=None):
        super(SentinelThread, self).__init__()
        self.logger = logging.getLogger(logger_name or __name__)
        self.pool = pool
        self.state = STOPPED

    def run(self):
        while self.state == RUNNING:
            if self.recycle_worker() > 0:
                self.revive_worker()
            time.sleep(0.5)

    def recycle_worker(self):
        recycled = 0
        for (pivot, w) in enumerate(self.pool.pool):
            if w.exitcode is not None:
                w.join()
                recycled += 1
                self.delete(pivot)
        return recycled

    def revive_worker(self):
        for _ in range(self.pool.pool_size - len(self.pool.pool)):
            w = Worker(self.pool)
            w.daemon = True
            w.start()
            self.add(w)

    def add(self, w):
        self.pool.pool.append(w)

    def delete(self, pivot):
        del self.pool.pool[pivot]

    def start(self):
        self.state = RUNNING
        super(SentinelThread, self).start()

    def stop(self):
        self.state = STOPPED


class ProcessPool(object):

    def __init__(self, pool_size=None, max_load_single_proc=3, interval=None,
                 logger_name=None):
        self.logger = logging.getLogger(logger_name or __name__)
        self.pool_size = pool_size if pool_size else self.get_default_size()
        self.max_load_single_proc = max_load_single_proc
        self.interval = interval
        self.reject_add_task = False
        self.task_queue = JoinableQueue(
            self.pool_size * self.max_load_single_proc)
        self.result_queue = JoinableQueue()
        self.exc_queue = JoinableQueue()
        self.pool = []
        self.state = RUNNING
        self.init_worker_pool()
        self.sentinel_thread()

    def spawn(self, worker_class, *args, **kwargs):
        if not callable(worker_class):
            raise ProcessPoolException('pool worker class must be callable')
        self.add_task((worker_class, args, kwargs))

    def add_task(self, task):
        if self.reject_add_task:
            raise ProcessPoolException('closed queue not allowed add task')
        self.task_queue.put(task)

    def get_default_size(self):
        cpu_cnt = cpu_count()
        return 2 if cpu_cnt <= 2 else int(cpu_cnt * 0.8) + 1

    def init_worker_pool(self):
        for _ in range(self.pool_size):
            w = Worker(self)
            w.daemon = True
            self.pool.append(w)
        for w in self.pool:
            w.start()

    def sentinel_thread(self):
        self.sentinel = SentinelThread(self)
        self.sentinel.daemon = True
        self.sentinel.start()

    def join_queue(self):
        self.reject_add_task = True
        self.task_queue.join()

    def stop(self):
        self.sentinel.stop()
        self.sentinel.join()
        for w in self.pool:
            w.stop()
        for w in self.pool:
            if w.is_alive():
                w.join()
        del self.pool[:]
        self.state = STOPPED

    def join(self, raise_error=False):
        self.join_queue()
        self.stop()
        if raise_error:
            try:
                exc, _ = self.exc_queue.get_nowait()
            except Queue.Empty:
                pass
            else:
                raise exc

    @property
    def exceptions(self):
        _exceptions = getattr(self, '_exceptions', [])
        if _exceptions:
            return _exceptions
        while True:
            try:
                err, tb = self.exc_queue.get_nowait()
                _exceptions.append((err, tb))
            except Queue.Empty:
                break
        self._exceptions = _exceptions
        return _exceptions

    @property
    def results(self):
        _results = getattr(self, '_results', [])
        if _results:
            return _results
        while True:
            try:
                res = self.result_queue.get_nowait()
                _results.append(res)
            except Queue.Empty:
                break
        self._results = _results
        return _results
