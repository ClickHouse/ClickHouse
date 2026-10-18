#!/usr/bin/env python3
"""Compare the normalized stripped ClickHouse binaries against the recorded hash."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.parse
import urllib.request


# Compiler and unstripped CMake reference reverified together in 82a328c8;
# the final normalized equality was recorded in 10ea5019.
EXPECTED_COMPILER = '52a7d0fdc59f6a0cd416844e55a70f67e689eeb1df0d98cc8c018444eb6262ce'
EXPECTED_CMAKE = 'cfbf99ef585fc2dbcf9f9ca413e13230ba2c9ab7dc8518df8bdeab06aa087cbc'
EXPECTED_PLAIN_STRIPPED = '6cd5519a79a8bc67fd3e1277af216b2dc327b045ad36abcf7535c50af89ff6fe'
EXPECTED_STRIPPED = 'ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585'
STRIP_FLAGS = ('--strip-all', '--remove-section=.note.gnu.build-id')


def sha256(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def compare(reference, original, strip, output, record):
    """Retain full, plain-stripped, and normalized hashes; cmp the normalized files."""
    cmake_stripped = output / 'cmake.stripped'
    bazel_stripped = output / 'bazel.stripped'
    record['CMAKE_REFERENCE_SHA256'] = sha256(reference)
    record['BAZEL_CLICKHOUSE_SHA256'] = sha256(original)
    record['STRIP_FLAGS'] = ' '.join(STRIP_FLAGS)
    record['EXPECTED_STRIPPED_SHA256'] = EXPECTED_STRIPPED

    for source, dest in ((reference, cmake_stripped), (original, bazel_stripped)):
        subprocess.run([str(strip), '--strip-all', '-o', str(dest), str(source)], check=True)
    record['CMAKE_PLAIN_STRIPPED_SHA256'] = sha256(cmake_stripped)
    record['BAZEL_PLAIN_STRIPPED_SHA256'] = sha256(bazel_stripped)
    for source, dest in ((reference, cmake_stripped), (original, bazel_stripped)):
        subprocess.run([str(strip), *STRIP_FLAGS, '-o', str(dest), str(source)], check=True)
    record['CMAKE_STRIPPED_SHA256'] = sha256(cmake_stripped)
    record['BAZEL_STRIPPED_SHA256'] = sha256(bazel_stripped)
    (output / 'stripped.sha256').write_text(
        f"{record['CMAKE_STRIPPED_SHA256']}  {cmake_stripped}\n"
        f"{record['BAZEL_STRIPPED_SHA256']}  {bazel_stripped}\n"
    )
    with (output / 'stripped-cmp.log').open('w') as log:
        comparison = subprocess.run(
            ['cmp', '--', str(cmake_stripped), str(bazel_stripped)],
            stdout=log, stderr=subprocess.STDOUT,
        )
    record['PARITY_STRIPPED_CMP_STATUS'] = comparison.returncode
    if comparison.returncode not in (0, 1):
        record['PARITY_STATUS'] = 2
    else:
        record['PARITY_STATUS'] = int(
            comparison.returncode != 0
            or record['CMAKE_STRIPPED_SHA256'] != EXPECTED_STRIPPED
            or record['BAZEL_STRIPPED_SHA256'] != EXPECTED_STRIPPED
        )
    return record


def write_summary(record, directory):
    content = ''.join(f'{key}={value}\n' for key, value in record.items())
    print(content, flush=True)
    (directory / 'stripped-summary.txt').write_text(content)


def recorded(root, output):
    execroot = (root / 'bazel-out').resolve().parent
    manifest = json.loads((root / 'bazel-bin/bazel/cmake_toolchain_files.json').read_text())
    record = {'CMAKE_COMPILER_SHA256': EXPECTED_COMPILER}
    # The reference installed llvm-strip as a hard link of the LLVM multicall
    # executable. Reproduce that installation from Bazel's selected toolchain.
    compiler = (execroot / manifest['cxx']).resolve(strict=True)
    record['BAZEL_COMPILER_SHA256'] = sha256(compiler)
    if record['BAZEL_COMPILER_SHA256'] != EXPECTED_COMPILER:
        raise ValueError('Bazel compiler differs from the recorded reference')
    if sha256(execroot / manifest['cc']) != EXPECTED_COMPILER:
        raise ValueError('Bazel C compiler differs from the recorded reference')
    strip = output / 'llvm-strip'
    try:
        os.link(compiler, strip)
    except OSError:
        shutil.copy2(compiler, strip)
    original = root / 'bazel-bin/clickhouse'
    reference = root / 'build_bazel_reference_x86_64/programs/clickhouse'
    if not reference.is_file() or sha256(reference) != EXPECTED_CMAKE:
        # This download is strictly a comparison input after Bazel finishes.
        # Keep it outside the checkout and Bazel's execroot.
        reference = output / 'clickhouse.cmake'
        if shutil.disk_usage(output).free < 4 * 1024**3:
            raise OSError('Not enough scratch disk for the two stripped copies and reference')
        key = os.environ.get('BUILDBUDDY_API_KEY')
        if not key:
            rc = root.parent / 'buildbuddy.bazelrc'
            if not rc.is_file():
                raise ValueError('Missing BuildBuddy runner credentials for the recorded reference')
            match = re.search(r'--remote_header=x-buildbuddy-api-key=([^\s]+)', rc.read_text())
            if not match:
                raise ValueError('Missing BuildBuddy remote header in runner configuration')
            key = match.group(1)
        params = urllib.parse.urlencode({
            'bytestream_url': f'bytestream://remote.buildbuddy.io/bb-snapshot/blobs/{EXPECTED_CMAKE}/929619736',
            'invocation_id': '4ea26abf-d8ee-4634-9d07-458a3645f3bc',
        })
        request = urllib.request.Request(
            'https://app.buildbuddy.io/file/download?' + params,
            headers={'x-buildbuddy-api-key': key, 'Accept-Encoding': 'identity'},
        )
        with urllib.request.urlopen(request, timeout=120) as response, reference.open('xb') as destination:
            shutil.copyfileobj(response, destination, 4 * 1024 * 1024)
        del key
    if sha256(reference) != EXPECTED_CMAKE:
        raise ValueError('CMake original does not match the recorded SHA256')
    print('CMAKE_REFERENCE_VERIFIED=' + EXPECTED_CMAKE, flush=True)
    compare(reference, original, strip, output, record)
    if record['CMAKE_PLAIN_STRIPPED_SHA256'] != EXPECTED_PLAIN_STRIPPED:
        print('PARITY_PREREQUISITE_FAILED=Plain llvm-strip output differs from the recorded comparison',
              file=sys.stderr)
        record['PARITY_STATUS'] = 2
    destination = os.environ.get('BUILDBUDDY_ARTIFACTS_DIRECTORY')
    write_summary(record, Path(destination) if destination else output)
    if destination and record['PARITY_STATUS']:
        from compare_elf_sections import sections

        bazel_sections = sections(output / 'bazel.stripped')
        (Path(destination) / 'bazel-stripped-sections.json').write_text(
            json.dumps(bazel_sections, indent=2) + '\n')
        (Path(destination) / 'cmake-stripped-sections.json').write_text(
            json.dumps(sections(output / 'cmake.stripped'), indent=2) + '\n')
        for section in bazel_sections:
            if section['name'] in ('.text', '.rodata', '.eh_frame', '.note.gnu.build-id'):
                print('PARITY_BAZEL_SECTION', section['name'], section['size'], section['sha256'], flush=True)
    return record['PARITY_STATUS']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('recorded', help='Compare against the recorded CMake reference, downloading it if needed')
    saved = commands.add_parser('compare', help='Compare the preserved CMake and Bazel executables')
    saved.add_argument('--cmake', type=Path, required=True)
    saved.add_argument('--bazel', type=Path, required=True)
    saved.add_argument('--strip', type=Path, required=True)
    saved.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == 'recorded':
            with tempfile.TemporaryDirectory(prefix='clickhouse-stripped-') as temporary:
                return recorded(Path.cwd(), Path(temporary))
        record = compare(args.cmake, args.bazel, args.strip, args.output, {})
        write_summary(record, args.output)
        return record['PARITY_STATUS']
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f'PARITY_STRIP_FAILED={error}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
