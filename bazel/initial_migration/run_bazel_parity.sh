#!/usr/bin/env bash
# Run on the Linux host that built build_bazel_reference_x86_64.
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: bash bazel/initial_migration/run_bazel_parity.sh [--stripped]

Build //:clickhouse using --config=cmake_match, compare
both complete executables, then capture the CMake and Bazel command audits.
Requires the preserved CMake reference and tool installation from
bazel/initial_migration/run_cmake_reference.sh. Refresh the saved Bazel metadata from that
reference with bazel/initial_migration/refresh_cmake_metadata.py before running this script.

--stripped hashes both originals, compares copies made with the same
llvm-strip --strip-all --remove-section=.note.gnu.build-id, and requires the
recorded stripped SHA256. On a mismatch, capture ELF and Rust archives.
It does not require full unstripped equality or run the command audit.

Exit 0: both full executables compare equal and both command audits pass,
        or --stripped is set and both stripped executables compare equal.
Exit 1: executable bytes or a command audit differ.
Exit 2: prerequisite, build, ELF capture, comparison, or audit execution failed.
Actual Bazel, cmp, and audit statuses are recorded separately in summary.txt.
EOF
}

if [[ ${1:-} == --help || ${1:-} == -h ]]; then
    usage
    exit 0
fi
mode=full
if [[ ${1:-} == --stripped ]] && (( $# == 1 )); then
    mode=stripped
elif (( $# != 0 )); then
    usage >&2
    exit 2
fi

root="$(git rev-parse --show-toplevel)"
cd "$root"
build="$root/build_bazel_reference_x86_64"
tools_dir="$root/build_bazel_reference_tools"
cmake_binary="$build/programs/clickhouse"
bazel_binary="$root/bazel-bin/clickhouse"
export SOURCE_DATE_EPOCH=1
export CARGO_HOME="$tools_dir/cargo"
export RUSTUP_HOME="$tools_dir/rustup"
export PATH="$tools_dir/llvm/bin:$tools_dir/python/bin:$tools_dir/yasm/usr/bin:$tools_dir/cmake/bin:$tools_dir/ninja:$CARGO_HOME/bin:$PATH"
export PYTHONDONTWRITEBYTECODE=1
hash -r

if [[ ! -d $build ]]; then
    echo "PARITY_PREREQUISITE_FAILED=Missing preserved CMake build: $build" >&2
    exit 2
fi
output="$(mktemp -d "$build/bazel-parity-XXXXXXXX")"
bazel_status=not-run
cmp_status=not-run
audit_status=not-run
elf_status=not-run
llvm_header_status=not-run
cmake_hash=not-verified
bazel_hash=not-built
stripped_cmp_status=not-run
stripped_cmake_hash=not-built
stripped_bazel_hash=not-built
plain_stripped_cmake_hash=not-built
plain_stripped_bazel_hash=not-built
strip_flags=not-run
compiler_hash=not-verified
bazel_compiler_hash=not-verified

# save_evidence is invoked by the EXIT trap below.
# shellcheck disable=SC2329
save_evidence() {
    status=$?
    trap - EXIT
    # A failed optional upload must never turn a failed comparison into success.
    set +e
    {
        printf 'CMAKE_COMPILER_SHA256=%s\n' "$compiler_hash"
        printf 'BAZEL_COMPILER_SHA256=%s\n' "$bazel_compiler_hash"
        printf 'CMAKE_REFERENCE_SHA256=%s\n' "$cmake_hash"
        printf 'BAZEL_CLICKHOUSE_SHA256=%s\n' "$bazel_hash"
        printf 'PARITY_MODE=%s\n' "$mode"
        printf 'CMAKE_STRIPPED_SHA256=%s\n' "$stripped_cmake_hash"
        printf 'BAZEL_STRIPPED_SHA256=%s\n' "$stripped_bazel_hash"
        printf 'CMAKE_PLAIN_STRIPPED_SHA256=%s\n' "$plain_stripped_cmake_hash"
        printf 'BAZEL_PLAIN_STRIPPED_SHA256=%s\n' "$plain_stripped_bazel_hash"
        printf 'STRIP_FLAGS=%s\n' "$strip_flags"
        printf 'PARITY_STRIPPED_CMP_STATUS=%s\n' "$stripped_cmp_status"
        printf 'BAZEL_BUILD_STATUS=%s\n' "$bazel_status"
        printf 'PARITY_CMP_STATUS=%s\n' "$cmp_status"
        printf 'PARITY_ELF_STATUS=%s\n' "$elf_status"
        printf 'PARITY_LLVM_HEADERS_STATUS=%s\n' "$llvm_header_status"
        printf 'PARITY_AUDIT_STATUS=%s\n' "$audit_status"
        printf 'PARITY_STATUS=%s\n' "$status"
        printf 'PARITY_EVIDENCE=%s\n' "$output"
    } | tee "$output/summary.txt"
    if [[ -n ${BUILDBUDDY_ARTIFACTS_DIRECTORY:-} ]]; then
        dest="$BUILDBUDDY_ARTIFACTS_DIRECTORY/clickhouse-bazel-parity"
        if ! mkdir -p "$dest"; then
            echo "PARITY_ARTIFACT_FAILED=$dest" >&2
            exit 2
        fi
        for file in "$output"/*; do
            [[ -f $file ]] || continue
            # The full hashes, cmp and ELF tables are the saved evidence.
            # Do not upload the two nearly gigabyte temporary executables.
            [[ $file == *.stripped ]] && continue
            if ! case "${file##*/}" in
                *.log|*.readelf.txt) gzip -c "$file" > "$dest/${file##*/}.gz" ;;
                *) cp "$file" "$dest/" ;;
            esac; then
                echo "PARITY_ARTIFACT_FAILED=$file" >&2
                exit 2
            fi
        done
    fi
    rm -f -- "$output/cmake.stripped" "$output/bazel.stripped"
    exit "$status"
}
trap 'save_evidence' EXIT

# compiler.sha256 names the recorded staged compiler, not an on-disk file;
# check the digest against the current installation explicitly.
for input in "$build/compiler.sha256" "$build/cmake.sha256" "$cmake_binary" \
             "$build/src/Storages/System/StorageSystemBuildOptions.generated.cpp" \
             "$build/base/base/GitHash.generated.cpp" \
             "$tools_dir/any2bazel/scripts/extract_cmake.py"; do
    if [[ ! -s $input ]]; then
        echo "PARITY_PREREQUISITE_FAILED=Missing $input" >&2
        exit 2
    fi
done
for tool in "$tools_dir/llvm/bin/clang" "$tools_dir/llvm/bin/clang++" \
            "$tools_dir/llvm/bin/llvm-readelf" "$tools_dir/python/bin/python3" \
            "$tools_dir/ninja/ninja" "$tools_dir/cmake/bin/cmake" \
            "$tools_dir/llvm/bin/llvm-strip"; do
    if [[ ! -x $tool ]]; then
        echo "PARITY_PREREQUISITE_FAILED=Missing executable $tool" >&2
        exit 2
    fi
done
read -r expected_compiler_hash _ < "$build/compiler.sha256"
read -r expected_cmake_hash _ < "$build/cmake.sha256"
if [[ ! $expected_compiler_hash =~ ^[0-9a-f]{64}$ || ! $expected_cmake_hash =~ ^[0-9a-f]{64}$ ]]; then
    echo "PARITY_PREREQUISITE_FAILED=Malformed compiler.sha256 or cmake.sha256" >&2
    exit 2
fi
if ! sha256sum "$tools_dir/llvm/bin/clang" "$tools_dir/llvm/bin/clang++" > "$output/compiler.sha256"; then
    echo "PARITY_PREREQUISITE_FAILED=Could not hash the recorded CMake compiler" >&2
    exit 2
fi
while read -r actual_hash actual_name; do
    if [[ $actual_hash != "$expected_compiler_hash" ]]; then
        echo "PARITY_PREREQUISITE_FAILED=$actual_name differs from compiler.sha256" >&2
        exit 2
    fi
done < "$output/compiler.sha256"
compiler_hash="$expected_compiler_hash"
if ! sha256sum "$cmake_binary" > "$output/cmake.sha256"; then
    echo "PARITY_PREREQUISITE_FAILED=Could not hash the CMake executable" >&2
    exit 2
fi
read -r actual_cmake_hash _ < "$output/cmake.sha256"
if [[ $actual_cmake_hash != "$expected_cmake_hash" ]]; then
    echo "PARITY_PREREQUISITE_FAILED=programs/clickhouse differs from cmake.sha256" >&2
    exit 2
fi
cmake_hash="$actual_cmake_hash"
if ! python3 bazel/initial_migration/refresh_cmake_metadata.py --arch=x86_64 --build-dir="$build" --check \
        > "$output/metadata-check.log" 2>&1; then
    echo "PARITY_PREREQUISITE_FAILED=Saved CMake metadata differs; see metadata-check.log" >&2
    tail -50 "$output/metadata-check.log" >&2
    exit 2
fi
echo "CMAKE_REFERENCE_VERIFIED=$cmake_hash"
echo "CMAKE_COMPILER_VERIFIED=$compiler_hash"

bazel_args=(
    --config=cmake_match
    --noannounce_rc --color=no --curses=no
    --show_progress_rate_limit=30
)
# Resolve the same target toolchain as //:clickhouse under the same Bazel
# configuration. Checking only the saved CMake compiler would miss a rebuild
# of Bazel's stage1 compiler after .bazelrc or LLVM patch changes.
if ! bazel --host_jvm_args=-Xmx24g build "${bazel_args[@]}" //bazel:cmake_toolchain_files \
        @clickhouse_llvm_project//:llvm_generated_headers \
        > "$output/bazel-toolchain-build.log" 2>&1; then
    echo 'PARITY_PREREQUISITE_FAILED=Could not build the selected Bazel C++ toolchain' >&2
    tail -70 "$output/bazel-toolchain-build.log" >&2
    exit 2
fi
if ! python3 - "$root" "$output" "$compiler_hash" <<'PY'
import hashlib
import json
from pathlib import Path
import shutil
import sys

root, output = map(Path, sys.argv[1:3])
expected = sys.argv[3]
manifest = root / 'bazel-bin/bazel/cmake_toolchain_files.json'
shutil.copyfile(manifest, output / 'bazel-toolchain.json')
tools = json.loads(manifest.read_text())
execroot = (root / 'bazel-out').resolve().parent
with (output / 'bazel-toolchain.sha256').open('w') as record:
    for tool in ('cc', 'cxx'):
        executable = execroot / tools[tool]
        digest = hashlib.sha256()
        with executable.open('rb') as binary:
            for chunk in iter(lambda: binary.read(1024 * 1024), b''):
                digest.update(chunk)
        actual = digest.hexdigest()
        print(actual, tool, file=record, flush=True)
        if actual != expected:
            raise SystemExit(f'{tool}: Bazel compiler {actual} differs from CMake compiler {expected}')
PY
then
    echo 'PARITY_PREREQUISITE_FAILED=Bazel and CMake compiler verification failed' >&2
    exit 2
fi
bazel_compiler_hash="$compiler_hash"
echo "BAZEL_COMPILER_VERIFIED=$bazel_compiler_hash"
if ! bazel --host_jvm_args=-Xmx24g cquery "${bazel_args[@]}" --output=files \
        @clickhouse_llvm_project//:llvm_generated_headers \
        > "$output/llvm-generated-headers.txt" 2> "$output/llvm-headers-query.log"; then
    echo 'PARITY_PREREQUISITE_FAILED=Could not enumerate Bazel LLVM generated headers' >&2
    tail -40 "$output/llvm-headers-query.log" >&2
    exit 2
fi
llvm_header_status=0
python3 - "$root" "$build" "$output" <<'PY' || llvm_header_status=$?
import hashlib
import json
from pathlib import Path
import sys

root, build, output = map(Path, sys.argv[1:])
execroot = (root / 'bazel-out').resolve().parent
files = (output / 'llvm-generated-headers.txt').read_text().splitlines()
if not files:
    raise SystemExit('Bazel returned no LLVM generated headers')
records = []
for filename in files:
    path = Path(filename)
    prefix, separator, relative = filename.partition('/gen/x86_64/')
    if not separator or '..' in Path(relative).parts or not relative.startswith('llvm/'):
        raise SystemExit('Unexpected generated LLVM header ' + filename)
    bazel = execroot / path
    cmake = build / 'contrib/llvm-project' / relative
    actual = hashlib.sha256(bazel.read_bytes()).hexdigest()
    expected = hashlib.sha256(cmake.read_bytes()).hexdigest() if cmake.is_file() else None
    records.append({
        'name': relative, 'bazel_sha256': actual, 'cmake_sha256': expected,
        'equal': actual == expected,
    })
report = {'compared': len(records), 'differences': sum(not r['equal'] for r in records), 'headers': records}
(output / 'llvm-generated-headers.json').write_text(json.dumps(report, indent=2) + '\n')
print('PARITY_LLVM_HEADERS=' + str(report['compared']) + ' differences=' + str(report['differences']))
raise SystemExit(1 if report['differences'] else 0)
PY
# Generated comments can differ while compiled bytes still agree. Record these
# differences for diagnosis; cmp below decides executable equality.
if [[ ! -s "$output/llvm-generated-headers.json" ]]; then
    echo 'PARITY_PREREQUISITE_FAILED=LLVM header comparison did not complete' >&2
    exit 2
fi
echo "PARITY_LLVM_HEADERS_STATUS=$llvm_header_status"
mapfile -t rust_targets < <(python3 - <<'PY'
import sys
sys.path.insert(0, 'bazel')
from initial_migration.rust_archives import ARCHIVE_TARGETS as RUST_ARCHIVES
for crate in RUST_ARCHIVES.values():
    print(f'//bazel/rust:{crate}_processed')
PY
)
if (( ${#rust_targets[@]} != 7 )); then
    echo 'PARITY_PREREQUISITE_FAILED=Could not enumerate the native Rust archives' >&2
    exit 2
fi
{
    printf 'SOURCE_DATE_EPOCH=%q ' "$SOURCE_DATE_EPOCH"
    printf '%q ' bazel --host_jvm_args=-Xmx24g build "${bazel_args[@]}" //:clickhouse "${rust_targets[@]}"
    printf '\n'
} > "$output/bazel-command.txt"
echo 'BAZEL_PARITY_BUILD_STARTED'
bazel_status=0
bazel --host_jvm_args=-Xmx24g build "${bazel_args[@]}" //:clickhouse "${rust_targets[@]}" \
    2>&1 | tee "$output/bazel-build.log" || bazel_status=$?
echo "BAZEL_BUILD_STATUS=$bazel_status"

result=0
if (( bazel_status != 0 )); then
    # bazel-bin/clickhouse could be left from an older invocation on failure.
    echo "BAZEL_PARITY_BUILD_FAILED=$bazel_status" >&2
    result=2
elif [[ ! -f $bazel_binary ]]; then
    echo "BAZEL_PARITY_BUILD_FAILED=Missing $bazel_binary" >&2
    result=2
elif ! sha256sum "$bazel_binary" > "$output/bazel.sha256"; then
    echo "BAZEL_PARITY_BUILD_FAILED=Could not hash Bazel executable" >&2
    result=2
else
    read -r bazel_hash _ < "$output/bazel.sha256"
    # Preserve the first differing byte from cmp, or its I/O error.
    cmp_status=0
    cmp -- "$cmake_binary" "$bazel_binary" > "$output/cmp.log" 2>&1 || cmp_status=$?
    echo "BAZEL_CLICKHOUSE_SHA256=$bazel_hash"
    echo "PARITY_CMP_STATUS=$cmp_status"
    if [[ $mode == stripped ]]; then
        python3 bazel/initial_migration/verify_stripped_hash.py compare --cmake "$cmake_binary" --bazel "$bazel_binary" \
            --strip "$tools_dir/llvm/bin/llvm-strip" --output "$output" \
            > "$output/stripped-verification.log" 2>&1 || result=$?
        cat "$output/stripped-verification.log"
        if [[ -f $output/stripped-summary.txt ]]; then
            while IFS='=' read -r name value; do
                case "$name" in
                    CMAKE_STRIPPED_SHA256) stripped_cmake_hash=$value ;;
                    BAZEL_STRIPPED_SHA256) stripped_bazel_hash=$value ;;
                    CMAKE_PLAIN_STRIPPED_SHA256) plain_stripped_cmake_hash=$value ;;
                    BAZEL_PLAIN_STRIPPED_SHA256) plain_stripped_bazel_hash=$value ;;
                    PARITY_STRIPPED_CMP_STATUS) stripped_cmp_status=$value ;;
                    STRIP_FLAGS) strip_flags=$value ;;
                esac
            done < "$output/stripped-summary.txt"
        fi
        if (( result == 0 )); then
            exit 0
        elif (( result != 1 )); then
            echo "PARITY_STRIP_FAILED=See stripped-verification.log" >&2
            result=2
        fi
    else
        case "$cmp_status" in
            0) ;;
            1) result=1 ;;
            *) result=2 ;;
        esac
    fi
fi

# ELF tables expose link order, section sizes, build IDs, needed libraries and
# symbol versions without mutating either executable. Do not dump huge .text.
elf_status=0
readelf_args=(--file-header --program-headers --sections --notes --dynamic --version-info --dyn-syms --wide)
"$tools_dir/llvm/bin/llvm-readelf" "${readelf_args[@]}" "$cmake_binary" \
    > "$output/cmake.readelf.txt" 2> "$output/cmake-readelf.log" || elf_status=$?
if (( bazel_status == 0 )) && [[ -f $bazel_binary ]]; then
    "$tools_dir/llvm/bin/llvm-readelf" "${readelf_args[@]}" "$bazel_binary" \
        > "$output/bazel.readelf.txt" 2> "$output/bazel-readelf.log" || elf_status=$?
    python3 bazel/initial_migration/compare_elf_sections.py "$cmake_binary" "$bazel_binary" \
        "$output/elf-sections.json" > "$output/elf-sections.log" 2>&1 || elf_status=$?
    cat "$output/elf-sections.log"
    if [[ $mode == stripped && $stripped_cmp_status != not-run ]]; then
        python3 bazel/initial_migration/compare_elf_sections.py "$output/cmake.stripped" "$output/bazel.stripped" \
            "$output/stripped-elf-sections.json" > "$output/stripped-elf-sections.log" 2>&1 || elf_status=$?
        cat "$output/stripped-elf-sections.log"
        if (( stripped_cmp_status == 1 )); then
            python3 bazel/initial_migration/compare_elf_symbols.py "$cmake_binary" "$bazel_binary" \
                "$output/elf-symbols.json.gz" > "$output/elf-symbols.log" 2>&1 || elf_status=$?
            cat "$output/elf-symbols.log"
        fi
    fi
    # These archives are built and downloaded as explicit targets above.
    # Hash native archives only as evidence, never as inputs to Bazel.
    python3 - "$root" "$build" "$output" <<'PY' || elf_status=$?
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import sys

root, build, output = map(Path, sys.argv[1:])
sys.path.insert(0, str(root / 'bazel'))
from initial_migration.rust_archives import ARCHIVE_TARGETS as RUST_ARCHIVES
ar = root / 'build_bazel_reference_tools/llvm/bin/llvm-ar'

def record(path):
    with path.open('rb') as file:
        digest = hashlib.file_digest(file, 'sha256').hexdigest()
    members = subprocess.check_output([str(ar), 't', str(path)], text=True).splitlines()
    return {'sha256': digest, 'bytes': path.stat().st_size, 'members': members}

records = {}
for relative, crate in RUST_ARCHIVES.items():
    native = record(build / relative)
    bazel = record(root / 'bazel-bin/bazel/rust' / ('lib' + crate + '_processed.a'))
    comparison = {
        'same_sha256': native['sha256'] == bazel['sha256'],
        'same_ordered_members': native['members'] == bazel['members'],
        'byte_delta': bazel['bytes'] - native['bytes'],
    }
    records[crate] = {'cmake': native, 'bazel': bazel, 'comparison': comparison}
    print('PARITY_RUST_ARCHIVE', crate, json.dumps(comparison), flush=True)
with gzip.open(output / 'rust-archives.json.gz', 'wt') as file:
    json.dump(records, file)
PY
    # Record the four libc++ replaceable allocation functions when a build
    # still contains __lcxx_override. Stream .symtab; do not retain a dump of
    # all 1.7 million ClickHouse symbols in the artifacts.
    python3 - "$cmake_binary" "$bazel_binary" "$tools_dir" "$output" <<'PY' || elf_status=$?
import json
from pathlib import Path
import subprocess
import sys

cmake, bazel, tools, output = map(Path, sys.argv[1:])
report = json.loads((output / 'elf-sections.json').read_text())
wanted = {'_Znwm', '_Znam', '_ZnwmSt11align_val_t', '_ZnamSt11align_val_t'}
has_override = any(s['name'] == '__lcxx_override'
                   for s in report['cmake_sections'] + report['bazel_sections'])
for side, binary in [('cmake', cmake), ('bazel', bazel)]:
    override = [s for s in report[side + '_sections'] if s['name'] == '__lcxx_override']
    if not has_override:
        continue
    symbols = []
    with subprocess.Popen([str(tools / 'llvm/bin/llvm-readelf'), '--symbols', '--wide', str(binary)],
                          stdout=subprocess.PIPE, text=True) as process:
        for line in process.stdout:
            fields = line.split()
            if len(fields) >= 8 and fields[-1].split('@', 1)[0] in wanted:
                symbols.append(line.rstrip())
        status = process.wait()
    if status:
        raise subprocess.CalledProcessError(status, process.args)
    (output / (side + '-allocation-symbols.json')).write_text(
        json.dumps({'section': override, 'symbols': symbols}, indent=2) + '\n')
    if override:
        with (output / (side + '-allocation-override.txt')).open('w') as destination:
            subprocess.run([str(tools / 'llvm/bin/llvm-objdump'),
                            '--disassemble', '--section=__lcxx_override', str(binary)],
                           stdout=destination, check=True)
PY
fi
if (( elf_status != 0 )); then
    echo "PARITY_ELF_FAILED=$elf_status" >&2
    result=2
fi

if [[ $mode == stripped ]]; then
    exit "$result"
fi

# Capture commands even when the build or binary comparison failed.
echo 'PARITY_COMMAND_AUDIT_STARTED'
audit_status=0
bash bazel/initial_migration/compare_cmake_actions.sh "$build" "$tools_dir/any2bazel" "$output/command-audit" \
    2>&1 | tee "$output/command-audit.log" || audit_status=$?
echo "PARITY_AUDIT_STATUS=$audit_status"
case "$audit_status" in
    0) ;;
    1) if (( result == 0 )); then result=1; fi ;;
    *) result=2 ;;
esac
exit "$result"
