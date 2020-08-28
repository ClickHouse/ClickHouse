import collections
import contextlib
import multiprocessing
import os
import shutil
import sys
import socket
import tempfile
import threading

import yaml


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


def read_md_file(path):
    in_meta = False
    meta = {}
    meta_text = []
    content = []
    if os.path.exists(path):
        with open(path, 'r') as f:
            for line in f:
                if line.startswith('---'):
                    if in_meta:
                        in_meta = False
                        meta = yaml.full_load(''.join(meta_text))
                    else:
                        in_meta = True
                else:
                    if in_meta:
                        meta_text.append(line)
                    else:
                        content.append(line)
    return meta, ''.join(content)


def write_md_file(path, meta, content):
    dirname = os.path.dirname(path)
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    with open(path, 'w') as f:
        if meta:
            print('---', file=f)
            yaml.dump(meta, f)
            print('---', file=f)
            if not content.startswith('\n'):
                print('', file=f)
        f.write(content)


def represent_ordereddict(dumper, data):
    value = []
    for item_key, item_value in data.items():
        node_key = dumper.represent_data(item_key)
        node_value = dumper.represent_data(item_value)

        value.append((node_key, node_value))

    return yaml.nodes.MappingNode(u'tag:yaml.org,2002:map', value)


yaml.add_representer(collections.OrderedDict, represent_ordereddict)
