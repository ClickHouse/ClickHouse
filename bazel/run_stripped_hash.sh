#!/usr/bin/env bash
# Build with RBE, then strip both recorded originals for comparison.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
export SOURCE_DATE_EPOCH=1
bazel --host_jvm_args=-Xmx24g build \
    --config=remote --config=x86_64 --//bazel:cmake_metadata \
    --remote_download_outputs=toplevel --noannounce_rc --color=no --curses=no \
    --show_progress_rate_limit=30 //:clickhouse //bazel:cmake_toolchain_files
python3 -B - <<'PY'
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

root = Path.cwd()
execroot = (root / 'bazel-out').resolve().parent
manifest = json.loads((root / 'bazel-bin/bazel/cmake_toolchain_files.json').read_text())
# These hashes were reverified together in BuildBuddy invocation 82a328c8.
expected_compiler = '52a7d0fdc59f6a0cd416844e55a70f67e689eeb1df0d98cc8c018444eb6262ce'
expected_cmake = 'cfbf99ef585fc2dbcf9f9ca413e13230ba2c9ab7dc8518df8bdeab06aa087cbc'
expected_plain_stripped = '6cd5519a79a8bc67fd3e1277af216b2dc327b045ad36abcf7535c50af89ff6fe'

def sha256(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()

record = {'CMAKE_COMPILER_SHA256': expected_compiler,
          'CMAKE_REFERENCE_SHA256': expected_cmake,
          'CMAKE_PLAIN_STRIPPED_SHA256': expected_plain_stripped,
          'STRIP_FLAGS': '--strip-all --remove-section=.note.gnu.build-id'}
with tempfile.TemporaryDirectory(prefix='clickhouse-stripped-') as temporary:
    temporary = Path(temporary)
    # The reference installed llvm-strip as a hard link of the LLVM multicall
    # executable. Reproduce that installation from Bazel's selected toolchain.
    compiler = (execroot / manifest['cxx']).resolve(strict=True)
    record['BAZEL_COMPILER_SHA256'] = sha256(compiler)
    if record['BAZEL_COMPILER_SHA256'] != expected_compiler:
        raise SystemExit('Bazel compiler differs from the recorded reference')
    if sha256(execroot / manifest['cc']) != expected_compiler:
        raise SystemExit('Bazel C compiler differs from the recorded reference')
    strip = temporary / 'llvm-strip'
    try:
        os.link(compiler, strip)
    except OSError:
        shutil.copy2(compiler, strip)
    original = root / 'bazel-bin/clickhouse'
    record['BAZEL_CLICKHOUSE_SHA256'] = sha256(original)
    reference = root / 'build_bazel_reference_x86_64/programs/clickhouse'
    if not reference.is_file() or sha256(reference) != expected_cmake:
        # This download is strictly a comparison input after Bazel finishes.
        # Keep it outside the checkout and Bazel's execroot.
        reference = temporary / 'clickhouse.cmake'
        if shutil.disk_usage(temporary).free < 4 * 1024**3:
            raise SystemExit('Not enough scratch disk for the two stripped copies and reference')
        key = os.environ.get('BUILDBUDDY_API_KEY')
        if not key:
            rc = root.parent / 'buildbuddy.bazelrc'
            if not rc.is_file():
                raise SystemExit('Missing BuildBuddy runner credentials for the recorded reference')
            match = re.search(r'--remote_header=x-buildbuddy-api-key=([^\s]+)', rc.read_text())
            if not match:
                raise SystemExit('Missing BuildBuddy remote header in runner configuration')
            key = match.group(1)
        params = urllib.parse.urlencode({
            'bytestream_url': 'bytestream://remote.buildbuddy.io/bb-snapshot/blobs/' + expected_cmake + '/929619736',
            'invocation_id': '4ea26abf-d8ee-4634-9d07-458a3645f3bc',
        })
        request = urllib.request.Request('https://app.buildbuddy.io/file/download?' + params,
            headers={'x-buildbuddy-api-key': key, 'Accept-Encoding': 'identity'})
        with urllib.request.urlopen(request, timeout=120) as response, reference.open('xb') as output:
            shutil.copyfileobj(response, output, 4 * 1024 * 1024)
        del key
    if sha256(reference) != expected_cmake:
        raise SystemExit('CMake original does not match the recorded SHA256')
    print('CMAKE_REFERENCE_VERIFIED=' + expected_cmake, flush=True)
    cmake_stripped = temporary / 'cmake.stripped'
    bazel_stripped = temporary / 'bazel.stripped'
    subprocess.run([str(strip), '--strip-all', '-o', str(cmake_stripped), str(reference)], check=True)
    subprocess.run([str(strip), '--strip-all', '-o', str(bazel_stripped), str(original)], check=True)
    if sha256(cmake_stripped) != expected_plain_stripped:
        raise SystemExit('Plain llvm-strip output does not match the original recorded comparison')
    record['BAZEL_PLAIN_STRIPPED_SHA256'] = sha256(bazel_stripped)
    flags = ['--strip-all', '--remove-section=.note.gnu.build-id']
    subprocess.run([str(strip), *flags, '-o', str(cmake_stripped), str(reference)], check=True)
    subprocess.run([str(strip), *flags, '-o', str(bazel_stripped), str(original)], check=True)
    record['CMAKE_STRIPPED_SHA256'] = sha256(cmake_stripped)
    record['BAZEL_STRIPPED_SHA256'] = sha256(bazel_stripped)
    comparison = subprocess.run(['cmp', '--silent', str(cmake_stripped), str(bazel_stripped)])
    record['PARITY_STRIPPED_CMP_STATUS'] = comparison.returncode
    record['PARITY_STATUS'] = int(
        comparison.returncode != 0 or record['BAZEL_STRIPPED_SHA256'] != record['CMAKE_STRIPPED_SHA256'])
    content = ''.join(str(key) + '=' + str(value) + '\n' for key, value in record.items())
    print(content, flush=True)
    destination = os.environ.get('BUILDBUDDY_ARTIFACTS_DIRECTORY')
    if destination:
        (Path(destination) / 'stripped-summary.txt').write_text(content)
        if record['PARITY_STATUS']:
            sys.path.insert(0, str(root / 'bazel'))
            from compare_elf_sections import sections
            bazel_sections = sections(bazel_stripped)
            (Path(destination) / 'bazel-stripped-sections.json').write_text(
                json.dumps(bazel_sections, indent=2) + '\n')
            (Path(destination) / 'cmake-stripped-sections.json').write_text(
                json.dumps(sections(cmake_stripped), indent=2) + '\n')
            for s in bazel_sections:
                if s['name'] in ('.text', '.rodata', '.eh_frame', '.note.gnu.build-id'):
                    print('PARITY_BAZEL_SECTION', s['name'], s['size'], s['sha256'], flush=True)
    raise SystemExit(record['PARITY_STATUS'])
PY
