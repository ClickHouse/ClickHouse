#!/usr/bin/env python3
"""Refresh --//bazel:cmake_metadata from one configured CMake architecture."""

import argparse
import ast
import difflib
from pathlib import Path
import re

from build_file import starlark
from generate_build import ARCHES, BUILD_OPTIONS_OUTPUT, ROOT, cmake_build_options

GIT_HASH_OUTPUT = 'base/base/GitHash.generated.cpp'


def _dict_value(node, key):
    if not isinstance(node, ast.Dict):
        raise ValueError(f'Expected a generated Starlark dictionary containing {key}')
    matches = [
        value for name, value in zip(node.keys, node.values)
        if isinstance(name, ast.Constant) and name.value == key
    ]
    if len(matches) != 1:
        raise ValueError(f'Expected exactly one {key} in generated Starlark, found {len(matches)}')
    return matches[0]


def _options(source, arch):
    # BUILD_OPTIONS contains repository_path() calls in unrelated flag fields.
    # Read only literal metadata; do not evaluate the loaded Starlark module.
    assignments = [
        statement.value for statement in ast.parse(source).body
        if isinstance(statement, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == 'BUILD_OPTIONS'
                for target in statement.targets)
    ]
    if len(assignments) != 1:
        raise ValueError('Expected exactly one BUILD_OPTIONS assignment')
    variant = _dict_value(assignments[0], arch)
    substitutions = _dict_value(variant, 'cmake_substitutions')
    values = ast.literal_eval(substitutions)
    if not isinstance(values, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in values.items()):
        raise ValueError('cmake_substitutions must be a literal string dictionary')
    template = ast.literal_eval(_dict_value(variant, 'template'))
    expected = 'config/' + arch + '/' + BUILD_OPTIONS_OUTPUT + '.in'
    if template != expected:
        raise ValueError(f'BUILD_OPTIONS[{arch!r}] template is {template!r}, expected {expected!r}')
    return template, substitutions, values


def _replace_substitutions(source, node, substitutions):
    # Python AST columns are UTF-8 byte offsets, including for non-ASCII text.
    lines = source.splitlines(keepends=True)
    start = sum(map(len, lines[:node.lineno - 1])) + node.col_offset
    end = sum(map(len, lines[:node.end_lineno - 1])) + node.end_col_offset
    return source[:start] + starlark(substitutions, indent=4).encode() + source[end:]


def _verify(options, template, git_hash, reference, reference_git, arch):
    _, _, substitutions = _options(options, arch)
    expanded = template.decode()
    for marker, value in substitutions.items():
        # ctx.actions.expand_template applies substitutions in dictionary order.
        expanded = expanded.replace(marker, value)
    if expanded.encode() != reference:
        diff = ''.join(difflib.unified_diff(
            reference.decode().splitlines(keepends=True),
            expanded.splitlines(keepends=True),
            fromfile='CMake/' + BUILD_OPTIONS_OUTPUT,
            tofile='Bazel/' + BUILD_OPTIONS_OUTPUT,
        ))
        raise ValueError('CMake build metadata differs from Bazel template expansion:\n' + diff)
    if git_hash != reference_git:
        raise ValueError('Bazel ' + GIT_HASH_OUTPUT + ' differs from the configured CMake file')
    source_hash = re.search(rb'^const char \* GIT_HASH = ("[^"]*");$', git_hash, re.MULTILINE)
    metadata_hash = re.search(rb'^    "GIT_HASH", ("[^"]*"),$', reference, re.MULTILINE)
    if not source_hash or not metadata_hash or source_hash[1] != metadata_hash[1]:
        raise ValueError('CMake GitHash.generated.cpp does not match its GIT_HASH build metadata')


def refresh(root, build, arch, check=False):
    options_file = root / 'bazel/build_options_data.bzl'
    options = options_file.read_bytes()
    template_name, substitutions_node, _ = _options(options, arch)
    template_file = root / 'bazel' / template_name
    git_file = root / 'bazel/config' / arch / GIT_HASH_OUTPUT
    reference = (build / BUILD_OPTIONS_OUTPUT).read_bytes()
    reference_git = (build / GIT_HASH_OUTPUT).read_bytes()

    if not check:
        template, substitutions = cmake_build_options(reference.decode())
        updated = _replace_substitutions(options, substitutions_node, substitutions)
        # Validate before changing any record, including when markers inside a
        # CMake value would be expanded again by Bazel's expand_template.
        _verify(updated, template.encode(), reference_git, reference, reference_git, arch)
        options_file.write_bytes(updated)
        template_file.write_bytes(template.encode())
        git_file.write_bytes(reference_git)

    # Check the exact records BUILD.bazel and build_options.bzl consume, also
    # after writing. --check verifies a saved record without modifying it.
    _verify(options_file.read_bytes(), template_file.read_bytes(), git_file.read_bytes(),
            reference, reference_git, arch)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--arch', choices=ARCHES, required=True)
    parser.add_argument('--build-dir', type=Path, required=True,
                        help='Configured CMake build directory containing generated C++ metadata.')
    parser.add_argument('--check', action='store_true',
                        help='Verify the saved Bazel metadata against CMake without writing files.')
    args = parser.parse_args()
    refresh(ROOT, args.build_dir, args.arch, check=args.check)
    print(f'{args.arch}: Bazel CMake metadata and GitHash.generated.cpp match {args.build_dir}')


if __name__ == '__main__':
    main()
