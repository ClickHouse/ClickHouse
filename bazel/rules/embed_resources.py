#!/usr/bin/env python3
"""Generate the two Web UI resource tables with deterministic gzip payloads."""

import argparse
import gzip
import json
from pathlib import Path
import re
import struct
import subprocess


def generate(source, output, namespace, compressed, cmake_gzip_mtime=None):
    if namespace == 'SQLConsole':
        index = source / 'index.html'
        if index.exists() and b'/ui/' not in index.read_bytes()[:65536]:
            raise ValueError('SQL Console assets must be built with base path /ui/')
    mime_types = {
        '.html': 'text/html; charset=UTF-8', '.htm': 'text/html; charset=UTF-8',
        '.js': 'application/javascript; charset=UTF-8', '.mjs': 'application/javascript; charset=UTF-8',
        '.css': 'text/css; charset=UTF-8', '.json': 'application/json; charset=UTF-8',
        '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
        '.svg': 'image/svg+xml', '.woff': 'font/woff', '.woff2': 'font/woff2',
        '.ico': 'image/x-icon', '.webp': 'image/webp',
        '.txt': 'text/plain; charset=UTF-8', '.xml': 'application/xml; charset=UTF-8',
    }
    # These two extensions are recognized only by the SQL Console CMake script.
    if namespace == 'SQLConsole':
        mime_types.update({'.gif': 'image/gif', '.ttf': 'font/ttf'})
    content = (f'// Auto-generated file - DO NOT EDIT\n// Generated from: {compressed}\n\n'
               '#pragma once\n#include <array>\n#include <string_view>\n\n'
               f'namespace DB::{namespace}\n{{\n\n'
               'struct EmbeddedResource\n{\n    std::string_view path;\n'
               '    const unsigned char* data;\n    size_t size;\n'
               '    std::string_view mime_type;\n};\n\n')
    entries = []
    size_checks = []
    variables = set()
    # WebUIRequestHandler uses lower_bound, so path ordering is significant.
    for path in sorted((p for p in source.rglob('*') if p.is_file()), key=lambda p: p.as_posix() + '.gz'):
        name = path.relative_to(source).as_posix()
        packed = output.parent / compressed / (name + '.gz')
        packed.parent.mkdir(parents=True, exist_ok=True)
        if cmake_gzip_mtime is None:
            with packed.open('wb') as raw:
                with gzip.GzipFile(filename='', mode='wb', fileobj=raw, compresslevel=6, mtime=0) as archive:
                    archive.write(path.read_bytes())
        else:
            # The CMake scripts use GNU gzip -c on the Git checkout. gzip stores
            # the checkout mtime and basename in its header. Bazel remote inputs
            # have normalized mtimes, so record the CMake mtimes as build metadata.
            with path.open('rb') as raw:
                archive = subprocess.run(
                    ['gzip', '-n', '-c'], stdin=raw, stdout=subprocess.PIPE, check=True
                ).stdout
            if archive[:10] != b'\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03':
                raise ValueError('CMake resources require GNU gzip at default compression on Linux')
            header = struct.pack('<4sIBB', b'\x1f\x8b\x08\x08', cmake_gzip_mtime, 0, 3)
            packed.write_bytes(header + path.name.encode('utf-8') + b'\0' + archive[10:])
        variable = re.sub('_+', '_', re.sub('[^a-zA-Z0-9_]', '_', 'resource_' + name + '.gz'))
        if variable in variables:
            raise ValueError('Resource C++ identifier collision: ' + variable)
        variables.add(variable)
        mime = mime_types.get(path.suffix.lower(), 'application/octet-stream')
        content += (f'// Resource: {name}\nconstexpr unsigned char {variable}[] = {{\n'
                    f'#embed {json.dumps(compressed + "/" + name + ".gz")}\n}};\n\n')
        entries.append('    EmbeddedResource{\n'
                       f'        {json.dumps(name)},\n        {variable},\n'
                       f'        sizeof({variable}),\n        {json.dumps(mime)}\n    }}')
        # Clang 22 can ignore a missing #embed when writing a dependency file.
        size_checks.append(f'static_assert(sizeof({variable}) == {packed.stat().st_size});')
    content += 'constexpr std::array embedded_resources = {\n' + ',\n'.join(entries) + '\n};\n\n'
    content += '\n'.join(size_checks) + '\n\n}\n'
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(content)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--namespace', choices=['SQLConsole', 'ClickStack'], required=True)
    parser.add_argument('--compressed', required=True)
    parser.add_argument('--cmake-gzip-mtime', type=int,
                        help='Git checkout mtime recorded from the CMake reference; use GNU gzip.')
    args = parser.parse_args()
    generate(args.source, args.output, args.namespace, args.compressed, args.cmake_gzip_mtime)
