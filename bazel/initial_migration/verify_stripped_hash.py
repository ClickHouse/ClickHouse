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


def compare(reference, original, strip, output, record, expected_hash=EXPECTED_STRIPPED):
    """Retain full, plain-stripped, and normalized hashes; cmp the normalized files."""
    cmake_stripped = output / 'cmake.stripped'
    bazel_stripped = output / 'bazel.stripped'
    record['CMAKE_REFERENCE_SHA256'] = sha256(reference)
    record['BAZEL_CLICKHOUSE_SHA256'] = sha256(original)
    record['CMAKE_REFERENCE_BYTES'] = reference.stat().st_size
    record['BAZEL_CLICKHOUSE_BYTES'] = original.stat().st_size
    record['STRIP_FLAGS'] = ' '.join(STRIP_FLAGS)
    record['EXPECTED_STRIPPED_SHA256'] = expected_hash

    for source, dest in ((reference, cmake_stripped), (original, bazel_stripped)):
        subprocess.run([str(strip), '--strip-all', '-o', str(dest), str(source)], check=True)
    record['CMAKE_PLAIN_STRIPPED_SHA256'] = sha256(cmake_stripped)
    record['BAZEL_PLAIN_STRIPPED_SHA256'] = sha256(bazel_stripped)
    for source, dest in ((reference, cmake_stripped), (original, bazel_stripped)):
        subprocess.run([str(strip), *STRIP_FLAGS, '-o', str(dest), str(source)], check=True)
    record['CMAKE_STRIPPED_SHA256'] = sha256(cmake_stripped)
    record['BAZEL_STRIPPED_SHA256'] = sha256(bazel_stripped)
    record['CMAKE_STRIPPED_BYTES'] = cmake_stripped.stat().st_size
    record['BAZEL_STRIPPED_BYTES'] = bazel_stripped.stat().st_size
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
            or record['CMAKE_STRIPPED_SHA256'] != expected_hash
            or record['BAZEL_STRIPPED_SHA256'] != expected_hash
            or record['CMAKE_STRIPPED_BYTES'] != record['BAZEL_STRIPPED_BYTES']
        )
    return record


def write_summary(record, directory):
    content = ''.join(f'{key}={value}\n' for key, value in record.items())
    print(content, flush=True)
    (directory / 'stripped-summary.txt').write_text(content)


def recorded(root, output, expected, invocation_id):
    execroot = (root / 'bazel-out').resolve().parent
    manifest = json.loads((root / 'bazel-bin/bazel/cmake_toolchain_files.json').read_text())
    record = {'PARITY_ARCH': expected['arch'], 'CMAKE_COMPILER_SHA256': expected['compiler_sha256']}
    # The reference installed llvm-strip as a hard link of the LLVM multicall
    # executable. Reproduce that installation from Bazel's selected toolchain.
    compiler = (execroot / manifest['cxx']).resolve(strict=True)
    record['BAZEL_COMPILER_SHA256'] = sha256(compiler)
    if record['BAZEL_COMPILER_SHA256'] != expected['compiler_sha256']:
        raise ValueError('Bazel compiler differs from the recorded reference')
    if sha256(execroot / manifest['cc']) != expected['compiler_sha256']:
        raise ValueError('Bazel C compiler differs from the recorded reference')
    strip = output / 'llvm-strip'
    try:
        os.link(compiler, strip)
    except OSError:
        shutil.copy2(compiler, strip)
    original = root / 'bazel-bin/clickhouse'
    reference = root / ('build_bazel_reference_' + expected['arch']) / 'programs/clickhouse'
    if not reference.is_file() or sha256(reference) != expected['cmake_sha256']:
        # This download is strictly a comparison input after Bazel finishes.
        # Keep it outside the checkout and Bazel's execroot.
        reference = output / 'clickhouse.cmake'
        if shutil.disk_usage(output).free < 4 * 1024**3:
            raise OSError('Not enough scratch disk for the two stripped copies and reference')
        if not invocation_id:
            raise ValueError('The CMake reference is absent; provide its BuildBuddy --reference-invocation')
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
            'bytestream_url': (
                f"bytestream://remote.buildbuddy.io/bb-snapshot/blobs/{expected['cmake_sha256']}/{expected['bytes']}"),
            'invocation_id': invocation_id,
        })
        request = urllib.request.Request(
            'https://app.buildbuddy.io/file/download?' + params,
            headers={'x-buildbuddy-api-key': key, 'Accept-Encoding': 'identity'},
        )
        with urllib.request.urlopen(request, timeout=120) as response, reference.open('xb') as destination:
            shutil.copyfileobj(response, destination, 4 * 1024 * 1024)
        del key
    if sha256(reference) != expected['cmake_sha256']:
        raise ValueError('CMake original does not match the recorded SHA256')
    print('CMAKE_REFERENCE_VERIFIED=' + expected['cmake_sha256'], flush=True)
    if elf_machine(reference) != expected['elf_machine']:
        raise ValueError('CMake reference ELF machine differs from the recorded architecture')
    if elf_machine(original) != expected['elf_machine']:
        raise ValueError('Bazel executable ELF machine differs from the recorded CMake reference')
    compare(reference, original, strip, output, record, expected['stripped_sha256'])
    if record['CMAKE_PLAIN_STRIPPED_SHA256'] != expected['plain_stripped_sha256']:
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


def elf_machine(path):
    with path.open('rb') as stream:
        header = stream.read(20)
    if len(header) != 20 or header[:5] != b'\x7fELF\x02' or header[5] not in (1, 2):
        raise ValueError(f'Expected ELF64 executable: {path}')
    return int.from_bytes(header[18:20], 'little' if header[5] == 1 else 'big')


def recorded_reference(args):
    if args.arch == 'x86_64':
        if args.reference_summary or args.reference_invocation:
            raise ValueError('The x86-64 reference is fixed; its summary and invocation cannot be overridden')
        return {
            'arch': 'x86_64',
            'compiler_sha256': EXPECTED_COMPILER,
            'cmake_sha256': EXPECTED_CMAKE,
            'plain_stripped_sha256': EXPECTED_PLAIN_STRIPPED,
            'stripped_sha256': EXPECTED_STRIPPED,
            'bytes': 929619736,
            'elf_machine': 62,
        }, '4ea26abf-d8ee-4634-9d07-458a3645f3bc'
    if not args.reference_summary:
        raise ValueError('--arch=aarch64 requires --reference-summary from the completed CMake build')
    expected = json.loads(args.reference_summary.read_text())
    if not isinstance(expected, dict) or expected.get('arch') != 'aarch64' or expected.get('elf_machine') != 183:
        raise ValueError('CMake --reference-summary must record an AArch64 ELF')
    for key in ('compiler_sha256', 'cmake_sha256', 'plain_stripped_sha256', 'stripped_sha256'):
        if not isinstance(expected.get(key), str) or not re.fullmatch(r'[0-9a-f]{64}', expected[key]):
            raise ValueError('Invalid CMake reference ' + key)
    if not isinstance(expected.get('bytes'), int) or expected['bytes'] < 20:
        raise ValueError('Invalid CMake reference original byte count')
    if not isinstance(expected.get('stripped_bytes'), int) or expected['stripped_bytes'] < 20:
        raise ValueError('Invalid CMake reference normalized byte count')
    if expected.get('strip_flags') != list(STRIP_FLAGS):
        raise ValueError('CMake reference used different llvm-strip flags')
    return expected, args.reference_invocation


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    snapshot = commands.add_parser('recorded', help='Compare against the recorded CMake reference, downloading if needed')
    snapshot.add_argument('--arch', choices=('x86_64', 'aarch64'), default='x86_64')
    snapshot.add_argument('--reference-summary', type=Path,
                          help='AArch64 cmake-reference-summary.json from bazel/initial_migration/run_cmake_reference.sh')
    snapshot.add_argument('--reference-invocation',
                          help='AArch64 BuildBuddy invocation containing the recorded clickhouse.cmake artifact')
    saved = commands.add_parser('compare', help='Compare the preserved CMake and Bazel executables')
    saved.add_argument('--cmake', type=Path, required=True)
    saved.add_argument('--bazel', type=Path, required=True)
    saved.add_argument('--strip', type=Path, required=True)
    saved.add_argument('--output', type=Path, required=True)
    saved.add_argument('--arch', choices=('x86_64', 'aarch64'), default='x86_64')
    saved.add_argument('--expected-sha256',
                       help='Required for AArch64: normalized SHA256 recorded after the full CMake build')
    args = parser.parse_args()
    try:
        if args.command == 'recorded':
            expected, invocation_id = recorded_reference(args)
            with tempfile.TemporaryDirectory(prefix='clickhouse-stripped-') as temporary:
                return recorded(Path.cwd(), Path(temporary), expected, invocation_id)
        if args.arch == 'x86_64':
            if args.expected_sha256 and args.expected_sha256 != EXPECTED_STRIPPED:
                raise ValueError('The x86-64 normalized SHA256 is fixed and cannot be overridden')
            expected_hash = EXPECTED_STRIPPED
        else:
            if not args.expected_sha256 or not re.fullmatch(r'[0-9a-f]{64}', args.expected_sha256):
                raise ValueError('--arch=aarch64 requires the recorded --expected-sha256')
            expected_hash = args.expected_sha256
        record = compare(args.cmake, args.bazel, args.strip, args.output,
                         {'PARITY_ARCH': args.arch}, expected_hash)
        write_summary(record, args.output)
        return record['PARITY_STATUS']
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f'PARITY_STRIP_FAILED={error}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
