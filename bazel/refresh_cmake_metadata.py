#!/usr/bin/env python3
"""Refresh --//bazel:cmake_metadata from one configured CMake architecture."""

import argparse
import difflib
from pathlib import Path
import re

from cmake_metadata import read_metadata, write_metadata
from generate_build import ARCHES, BUILD_OPTIONS_OUTPUT, ROOT, cmake_build_options

GIT_HASH_OUTPUT = 'base/base/GitHash.generated.cpp'


def _verify(substitutions, template, git_hash, reference, reference_git):
    if not isinstance(substitutions, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in substitutions.items()):
        raise ValueError('CMAKE_METADATA build_options must be a literal string dictionary')
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
    metadata = read_metadata(root)
    template_file = root / 'bazel/config' / arch / (BUILD_OPTIONS_OUTPUT + '.in')
    git_file = root / 'bazel/config' / arch / GIT_HASH_OUTPUT
    reference = (build / BUILD_OPTIONS_OUTPUT).read_bytes()
    reference_git = (build / GIT_HASH_OUTPUT).read_bytes()

    if not check:
        template, substitutions = cmake_build_options(reference.decode())
        # Validate before changing any record, including when markers inside a
        # CMake value would be expanded again by Bazel's expand_template.
        _verify(substitutions, template.encode(), reference_git, reference, reference_git)
        metadata[arch]['build_options'] = substitutions
        write_metadata(root, metadata)
        template_file.write_bytes(template.encode())
        git_file.write_bytes(reference_git)

    # Check the exact records BUILD.bazel and build_options.bzl consume, also
    # after writing. --check verifies a saved record without modifying it.
    _verify(read_metadata(root)[arch]['build_options'], template_file.read_bytes(), git_file.read_bytes(),
            reference, reference_git)


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
