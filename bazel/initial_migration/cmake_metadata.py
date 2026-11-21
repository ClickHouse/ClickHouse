"""Read and write the literal CMake reference data consumed by Bazel."""

import ast

from initial_migration.build_file import starlark


def read_metadata(root):
    filename = root / 'bazel/initial_migration/cmake_metadata.bzl'
    statements = ast.parse(filename.read_text(), filename=str(filename)).body
    if (len(statements) != 1 or not isinstance(statements[0], ast.Assign)
            or [getattr(target, 'id', None) for target in statements[0].targets] != ['CMAKE_METADATA']):
        raise ValueError(f'{filename} must contain only the literal CMAKE_METADATA assignment')
    return ast.literal_eval(statements[0].value)


def write_metadata(root, metadata):
    (root / 'bazel/initial_migration/cmake_metadata.bzl').write_text(
        '# CMake reference metadata; refreshed by bazel/initial_migration/refresh_cmake_metadata.py.\n'
        'CMAKE_METADATA = ' + starlark(metadata) + '\n')
