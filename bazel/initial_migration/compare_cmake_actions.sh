#!/usr/bin/env bash
# Compare commands for the native CMake clickhouse target and Bazel //:clickhouse.
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: bazel/initial_migration/compare_cmake_actions.sh CMAKE_BUILD ANY2BAZEL_CHECKOUT [OUTPUT_DIR]

Run on the Linux host that configured CMAKE_BUILD, with BuildBuddy configured
for Bazel and the reference Ninja on PATH. The default OUTPUT_DIR is a new
action-audit-<UTC timestamp> directory in CMAKE_BUILD; an explicit directory
must not already exist. Run after Bazel's cmake_metadata values are updated
from this CMake reference.

Exit 0: both command audits report no discrepancies within their documented
scope. Exit 1: either audit reports discrepancies. Exit 2: capture, extraction
or audit execution failed. No exit code verifies binary equality.
Reports and raw commands are retained for every outcome. On BuildBuddy they
are also uploaded from BUILDBUDDY_ARTIFACTS_DIRECTORY.
EOF
}

if [[ ${1:-} == --help || ${1:-} == -h ]]; then
    usage
    exit 0
fi
if (( $# < 2 || $# > 3 )); then
    usage >&2
    exit 2
fi
if [[ ! -d $1 || ! -d $2 ]]; then
    echo "CMAKE_BUILD and ANY2BAZEL_CHECKOUT must be existing directories" >&2
    exit 2
fi
build="$(cd "$1" && pwd -P)"
any2bazel="$(cd "$2" && pwd -P)"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
output="${3:-$build/action-audit-$(date -u +%Y%m%dT%H%M%SZ)}"

for input in "$build/build.ninja" "$build/compile_commands.json" \
             "$build/trace.jsonl" "$build/.cmake/api/v1/reply" \
             "$any2bazel/scripts/extract_cmake.py" "$any2bazel/scripts/extract_bazel.py" \
             "$any2bazel/scripts/diff.py" "$any2bazel/scripts/triage.py"; do
    if [[ ! -e $input ]]; then
        echo "Missing required audit input: $input" >&2
        exit 2
    fi
done
if ! mkdir "$output"; then
    echo "OUTPUT_DIR must be a new directory with an existing parent: $output" >&2
    exit 2
fi
output="$(cd "$output" && pwd -P)"
cd "$root"
export PYTHONDONTWRITEBYTECODE=1

stage() {
    printf 'CMAKE_ACTION_AUDIT=%s %s\n' "$(date -u +%FT%TZ)" "$1"
}

save_evidence() {
    status=$?
    trap - EXIT
    # Only compare_cmake_actions.py report may declare audit disagreement or
    # agreement. Earlier command failures remain distinct from discrepancies.
    if [[ ! -s "$output/summary.json" ]]; then status=2; fi
    echo "$status" > "$output/exit-status.txt"
    if [[ -n "${BUILDBUDDY_ARTIFACTS_DIRECTORY:-}" ]]; then
        stage "uploading captured commands and reports"
        dest="$BUILDBUDDY_ARTIFACTS_DIRECTORY/clickhouse-command-audit"
        mkdir -p "$dest"
        for file in "$output"/*; do
            [[ -f $file ]] || continue
            case "${file##*/}" in
                *.json|*.txt) gzip -c "$file" > "$dest/${file##*/}.gz" ;;
                *) cp "$file" "$dest/" ;;
            esac
        done
    fi
    echo "CMake/Bazel command evidence: $output"
    echo "Command comparisons do not verify binary equality."
    exit "$status"
}
trap save_evidence EXIT

{
    git rev-parse HEAD
    git -C "$any2bazel" rev-parse HEAD
    cmake --version
    ninja --version
    python3 --version
} > "$output/versions.txt"

# This target traversal has the original argument order, including link
# objects, archive operands and --whole-archive boundaries.
stage "ninja clickhouse commands and CMake File API"
ninja -C "$build" -t commands clickhouse > "$output/ninja-commands.txt"
ninja -C "$build" -t query programs/clickhouse > "$output/ninja-clickhouse-query.txt"
cp "$build/compile_commands.json" "$output/compile_commands.json"
python3 "$any2bazel/scripts/extract_cmake.py" "$build" "$root" \
    "$output/model.cmake.full.json" "$build/trace.jsonl" > "$output/extract-cmake.log" 2>&1

# The raw model is preserved. Scope the CMake comparison to clickhouse and
# its dependency closure, including Ninja direct object inputs.
stage "CMake clickhouse dependencies"
python3 bazel/initial_migration/compare_cmake_actions.py scope "$output"

bazel_args=(
    --config=cmake_match
    --include_commandline --include_artifacts --noinclude_param_files
    --noannounce_rc --color=no --curses=no --output=jsonproto
)
# Bazel 9.3.0 SpawnAction.getArguments() returns expanded Starlark Args.
# --include_param_files would flatten every CppCompile's original header
# depset. The extraction below rejects any remaining unresolved @ argument;
# it must never treat an incomplete command as a match.
bazel_cmd=(bazel --host_jvm_args=-Xmx24g)
aquery=(timeout --kill-after=60s 20m "${bazel_cmd[@]}" aquery)
"${bazel_cmd[@]}" info output_base > "$output/bazel-output-base.txt"
native_query='mnemonic("CppCompile|ObjcCompile|CppLink|CppArchive|ClickHouseNativeLink|ClickHouseNativeArchive|ClickHouseRustArchive|ClickHouseRustSymbols|ClickHouseCargo|Symlink", deps(//:clickhouse))'
assembly_query='mnemonic("Assemble|PreprocessAssemble|NasmCompile", deps(//:clickhouse)) union mnemonic("Genrule", //bazel:generate_x86_64_hdfs_crc)'
# Restrict FileWrite contents to the real MRI scripts, not every generated
# header and Rust manifest in deps(//:clickhouse).
mri_query='outputs(".*_archives/.*[.]mri", mnemonic("FileWrite", //:clickhouse))'
{
    printf '%q ' "${aquery[@]}" "${bazel_args[@]}" "$native_query"
    printf '\n'
    printf '%q ' "${aquery[@]}" "${bazel_args[@]}" "$assembly_query"
    printf '\n'
    printf '%q ' "${aquery[@]}" "${bazel_args[@]}" --include_file_write_contents "$mri_query"
    printf '\n'
} > "$output/bazel-commands.txt"
stage "Bazel C++ compile and link actions"
"${aquery[@]}" "${bazel_args[@]}" "$native_query" \
    > "$output/native.aquery.json" 2> "$output/native-aquery.log"
stage "Bazel NASM actions"
"${aquery[@]}" "${bazel_args[@]}" "$assembly_query" \
    > "$output/assembly.aquery.json" 2> "$output/assembly-aquery.log"
stage "Bazel archive MRI files"
"${aquery[@]}" "${bazel_args[@]}" --include_file_write_contents "$mri_query" \
    > "$output/mri.aquery.json" 2> "$output/mri-aquery.log"

# Keep actual labels and mnemonic/argv provenance separate from stock
# any2bazel's executable attribution. model.bazel.json retains native labels.
stage "any2bazel Bazel extraction"
python3 bazel/initial_migration/compare_cmake_actions.py adapt "$output"
python3 "$any2bazel/scripts/extract_bazel.py" "$output/native.any2bazel.aquery.json" "$root" \
    "$output/model.bazel.json" > "$output/extract-bazel.log" 2>&1
python3 bazel/initial_migration/compare_cmake_actions.py project "$output" --any2bazel "$any2bazel"

# EngFlow/any2bazel diff.py returns zero even when its report contains errors.
stage "any2bazel diff"
python3 "$any2bazel/scripts/diff.py" "$output/model.cmake.clickhouse.json" \
    "$output/model.bazel.any2bazel.json" "$root/bazel/initial_migration/any2bazel.json" > "$output/stock-diff.json" 2> "$output/stock-diff.log"
python3 "$any2bazel/scripts/triage.py" "$output/stock-diff.json" \
    > "$output/stock-triage.txt" 2> "$output/stock-triage.log"

audit_status=0
stage "native archive and object audit"
python3 bazel/initial_migration/audit_native_actions.py --any2bazel "$any2bazel" \
    --cmake-model "$output/model.cmake.clickhouse.json" --cmake-build "$build" \
    --bazel-model "$output/model.bazel.json" --config "$root/bazel/initial_migration/any2bazel.json" \
    --bazel-output-base "$(cat "$output/bazel-output-base.txt")" \
    --native-aquery "$output/native.aquery.json" --mri-aquery "$output/mri.aquery.json" \
    --assembly-aquery "$output/assembly.aquery.json" --output "$output/native-actions.json" \
    > "$output/native-actions.log" 2>&1 || audit_status=$?
python3 bazel/initial_migration/compare_cmake_actions.py report "$output" --native-status "$audit_status"
