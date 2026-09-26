#!/usr/bin/env bash
# Runs the wide-row parquet memory tests from test_storage_s3 against several
# released ClickHouse versions, once per Parquet reader, and prints one row per test.
#
# Each version is checked out at its own tag, because an older server binary
# rejects a newer programs/server/config.xml and never starts. The two test
# functions are taken from this checkout and appended to the tag's copy of
# test_storage_s3/test.py.
#
# Usage: tests/integration/test_storage_s3/wide_rows_memory_matrix.sh [version:tag ...]
set -euo pipefail

VERSIONS=("${@:-}")
if [ -z "${VERSIONS[0]}" ]; then
    VERSIONS=("26.2:v26.2.19.43-stable" "26.8:v26.8.2.7-lts")
fi

SELF=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SOURCE_TEST="$SELF/test.py"
REPO_ROOT=$(git -C "$SELF" rev-parse --show-toplevel)
WORK=${WIDE_ROWS_WORK_DIR:-$REPO_ROOT/tmp/wide-rows-matrix}
# `input_format_parquet_use_native_reader_v3` selects Arrow or v3 only on releases that
# still declare it; where it is obsolete there is one reader, and it runs once.
READERS=(
    "arrow:input_format_parquet_use_native_reader_v3=0"
    "v3:input_format_parquet_use_native_reader_v3=1"
)
TESTS=(
    "test_storage_s3/test.py::test_parquet_wide_rows_read_memory"
    "test_storage_s3/test.py::test_parquet_wide_rows_insert_memory"
)

extract_block() {
    python3 - "$SOURCE_TEST" <<'PY'
import sys
s = open(sys.argv[1]).read()
start = s.index("def _write_wide_parquet(")
end = s.index('@pytest.mark.parametrize("format_name", ["Parquet", "ORC"])')
sys.stdout.write(s[start:end])
PY
}

report() {
    python3 - "$1" "$2" "$3" <<'PY'
import glob, json, os, sys
label, root, started_at = sys.argv[1], sys.argv[2], float(sys.argv[3])
# Only results this run wrote: a previous run's file would otherwise be reported as this
# version's, silently, and it is the newest file that wins.
files = [f for f in glob.glob(os.path.join(root, "ci/tmp/result_integration_tests_*.json"))
         if os.path.getmtime(f) >= started_at]
if not files:
    print(f"{label}\t(no result file from this run)\t-")
    raise SystemExit
run = json.load(open(sorted(files, key=os.path.getmtime)[-1]))
for r in run.get("results") or []:
    name = (r.get("name") or "").replace("test_storage_s3/test.py::", "")
    print(f"{label}\t{name}\t{r.get('status')}")
# A run can write some rows and still have broken: pytest reports partial results
# alongside a top-level ERROR. Those rows are not a shorter matrix, they are an
# incomplete one, so the run's own status has to appear next to them.
status = run.get("status")
if status not in ("OK", "FAIL"):
    info = run.get("info")
    if isinstance(info, (list, tuple)):
        info = " ".join(str(i) for i in info)
    info = " ".join(str(info or "").split())[:200]
    print(f"{label}\t(run status {status}{': ' + info if info else ''})\t-")
PY
}

mkdir -p "$WORK"
BLOCK=$(extract_block)
RESULTS="$WORK/results.tsv"
: > "$RESULTS"

for entry in "${VERSIONS[@]}"; do
    version=${entry%%:*}
    tag=${entry#*:}
    root="$WORK/$tag"
    echo "=== $version ($tag)"

    if [ ! -d "$root/.git" ]; then
        git clone --depth 1 --filter=blob:none --sparse -b "$tag" \
            https://github.com/ClickHouse/ClickHouse.git "$root"
        git -C "$root" sparse-checkout set tests/integration ci programs/server docker/test
    fi

    target="$root/tests/integration/test_storage_s3/test.py"

    # The checkout is pinned to an exact tag, so the binary has to be the matching
    # exact release: the floating `26.2` image moves and would pair an old source
    # tree with a newer server.
    image_version=${tag#v}
    image_version=${image_version%%-*}

    mkdir -p "$root/ci/tmp" "$WORK/bin"
    binary="$WORK/bin/clickhouse-$image_version"
    if [ ! -s "$binary" ]; then
        cid=$(docker create "clickhouse/clickhouse-server:$image_version")
        docker cp "$cid:/usr/bin/clickhouse" "$binary"
        docker rm -f "$cid" > /dev/null
    fi
    cp "$binary" "$root/ci/tmp/clickhouse"

    root_abs=$(cd "$root" && pwd -P)

    readers=("${READERS[@]}")
    if git -C "$root" show HEAD:src/Core/FormatFactorySettings.h \
        | grep -q 'MAKE_OBSOLETE(M, Bool, input_format_parquet_use_native_reader_v3'; then
        readers=("v3:")
    fi

    for reader in "${readers[@]}"; do
        name=${reader%%:*}
        setting=${reader#*:}
        label="$version/$name"
        log="$WORK/$version-$name.log"
        echo "--- $label"

        # Reset the test file before appending, so that a checkout left over from an earlier
        # run picks up edits to the tests instead of keeping the copy it was first given.
        git -C "$root" checkout -- tests/integration/test_storage_s3/test.py
        printf '\n\n%s' "$BLOCK" >> "$target"
        if [ -n "$setting" ]; then
            python3 - "$target" "$setting" <<'SETTINGS'
import sys
path, setting = sys.argv[1], sys.argv[2]
s = open(path).read()
old = 'WIDE_ROWS_EXTRA_SETTINGS = ""'
if old not in s:
    raise SystemExit(f"{old} not found in {path}")
open(path, "w").write(s.replace(old, f'WIDE_ROWS_EXTRA_SETTINGS = ", {setting}"'))
SETTINGS
        fi

        # Only the praktika containers bound to this checkout. An unrelated job of
        # another worktree or another user shares the name prefix and must survive.
        for container in $(docker ps -aq --filter name=praktika); do
            if docker inspect -f '{{range .Mounts}}{{println .Source}}{{end}}' "$container" 2>/dev/null \
                | grep -qx "$root_abs"; then
                docker rm -f "$container" > /dev/null 2>&1 || true
            fi
        done

        started_at=$(python3 -c 'import time; print(time.time())')
        run_status=0
        ( cd "$root" && python3 -u -m ci.praktika run integration --test "${TESTS[@]}" ) \
            > "$log" 2>&1 || run_status=$?

        if grep -q "failed to start" "$log"; then
            echo -e "$label\t(server did not start)\t-" >> "$RESULTS"
        else
            report "$label" "$root" "$started_at" >> "$RESULTS"
            # A non-zero runner status with no test rows means the run itself broke, which
            # is not the same as a test failing and must not be reported as a pass. Rows in
            # parentheses are the script's own notes, not test results.
            if [ "$run_status" -ne 0 ] && ! grep -q "^$label	[^(]" "$RESULTS"; then
                echo -e "$label\t(runner exited $run_status, see $log)\t-" >> "$RESULTS"
            fi
        fi
    done
done

echo
printf '%-16s %-58s %s\n' VERSION/READER TEST STATUS
while IFS=$'\t' read -r v t s; do printf '%-16s %-58s %s\n' "$v" "$t" "$s"; done < "$RESULTS"
