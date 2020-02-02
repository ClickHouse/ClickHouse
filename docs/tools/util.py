import contextlib
import multiprocessing
import os
import shutil
import tempfile
import threading


@contextlib.contextmanager
def temp_dir():
    path = tempfile.mkdtemp(dir=os.environ.get('TEMP'))
    try:
        yield path
    finally:
        shutil.rmtree(path)


@contextlib.contextmanager
def autoremoved_file(path):
    try:
        with open(path, 'w') as handle:
            yield handle
    finally:
        os.unlink(path)


def run_function_in_parallel(func, args_list, threads=False):
    processes = []
    for task in args_list:
        cls = threading.Thread if threads else multiprocessing.Process
        processes.append(cls(target=func, args=task))
        processes[-1].start()
    for process in processes:
        process.join()
