import contextlib
import multiprocessing
import os
import shutil
import sys
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
    exit_code = 0
    for task in args_list:
        cls = threading.Thread if threads else multiprocessing.Process
        processes.append(cls(target=func, args=task))
        processes[-1].start()
    for process in processes:
        process.join()
        if not threads:
            if process.exitcode and not exit_code:
                exit_code = process.exitcode
    if exit_code:
        sys.exit(exit_code)
