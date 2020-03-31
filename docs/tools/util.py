import contextlib
import multiprocessing
import os
import shutil
import sys
import socket
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


@contextlib.contextmanager
def cd(new_cwd):
    old_cwd = os.getcwd()
    os.chdir(new_cwd)
    try:
        yield
    finally:
        os.chdir(old_cwd)


def get_free_port():
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def run_function_in_parallel(func, args_list, threads=False):
    for task in args_list:
        func(*task)
# TODO: back to parallel
#     processes = []
#     exit_code = 0
#     for task in args_list:
#         cls = threading.Thread if threads else multiprocessing.Process
#         processes.append(cls(target=func, args=task))
#         processes[-1].start()
#     for process in processes:
#         process.join()
#         if not threads:
#             if process.exitcode and not exit_code:
#                 exit_code = process.exitcode
#     if exit_code:
#         sys.exit(exit_code)
