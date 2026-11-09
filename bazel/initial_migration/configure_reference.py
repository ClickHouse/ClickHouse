#!/usr/bin/env python3
"""Configure the complete CMake reference graph and extract any2bazel evidence."""

import argparse
import json
import os
from pathlib import Path
import platform
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--arch', choices=('x86_64', 'aarch64'), required=True)
    parser.add_argument('--llvm-root', type=Path, required=True)
    parser.add_argument('--any2bazel', type=Path, required=True)
    parser.add_argument('--nasm', type=Path, required=True)
    parser.add_argument('--cmake-arg', action='append', default=[],
                        help='Additional CMake argument; repeat as --cmake-arg=-DNAME=VALUE.')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    build = root / ('build_bazel_reference_' + args.arch)
    query = build / '.cmake/api/v1/query'
    query.mkdir(parents=True, exist_ok=True)
    (query / 'codemodel-v2').touch()
    llvm = args.llvm_root.resolve()
    cross_compiling = args.arch != platform.machine()
    codegen_hook = ('-DCMAKE_PROJECT_INCLUDE=' + str(root / 'bazel/initial_migration/reference_codegen.cmake')
                    if cross_compiling else '-UCMAKE_PROJECT_INCLUDE')
    command = [
        'cmake', '-S', str(root), '-B', str(build), '-G', 'Ninja',
        '-DCMAKE_BUILD_TYPE=Release',
        '-DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-' + args.arch + '.cmake',
        '-DCMAKE_C_COMPILER=' + str(llvm / 'bin/clang'),
        '-DCMAKE_CXX_COMPILER=' + str(llvm / 'bin/clang++'),
        '-DCMAKE_ASM_COMPILER=' + str(llvm / 'bin/clang'),
        '-DCOMPILER_CACHE=disabled',
        '-DENABLE_LIBRARIES=ON', '-DENABLE_RUST=ON',
        '-DENABLE_TESTS=ON', '-DENABLE_CLICKHOUSE_ALL=ON',
        '-DCMAKE_EXPORT_COMPILE_COMMANDS=ON',
        # Unset a cached cross-graph hook for an ordinary native configuration.
        codegen_hook,
        '--trace-expand', '--trace-format=json-v1',
    ] + args.cmake_arg
    environment = dict(os.environ)
    environment['PATH'] = os.pathsep.join([
        str(llvm / 'bin'), str(args.nasm.resolve().parent), environment['PATH'],
    ])
    if cross_compiling:
        # The upstream cross-build configures host code generators in this
        # directory, but does not forward COMPILER_CACHE to that configure.
        native = build / 'native'
        native.mkdir(parents=True, exist_ok=True)
        native_command = [
            'cmake', '-S', str(root), '-B', str(native), '-G', 'Ninja',
            '-DCMAKE_BUILD_TYPE=Release', '-DCOMPILER_CACHE=disabled',
            '-DCMAKE_C_COMPILER=' + str(llvm / 'bin/clang'),
            '-DCMAKE_CXX_COMPILER=' + str(llvm / 'bin/clang++'),
            '-DENABLE_RUST=OFF',
        ]
        with (native / 'configure.log').open('w') as output:
            subprocess.run(native_command, cwd=root, env=environment,
                           stdout=output, stderr=subprocess.STDOUT, check=True)
    (build / 'configure-command.json').write_text(json.dumps(command, indent=2) + '\n')
    with (build / 'configure.log').open('w') as output, (build / 'trace.jsonl').open('w') as trace:
        subprocess.run(command, cwd=root, env=environment, stdout=output, stderr=trace, check=True)
    scripts = args.any2bazel.resolve() / 'scripts'
    subprocess.run([
        sys.executable, str(scripts / 'extract_cmake.py'),
        str(build), str(root), str(build / 'model.cmake.json'), str(build / 'trace.jsonl'),
    ], env=dict(environment, PYTHONPATH=str(scripts)), check=True)
    print(build / 'model.cmake.json')


if __name__ == '__main__':
    main()
