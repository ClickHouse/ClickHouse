#!/usr/bin/env bash
# Runs the wide-row parquet memory tests from test_storage_s3 against several
# released ClickHouse versions and prints one row per test.
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
WORK=${WIDE_ROWS_WORK_DIR:-/tmp/ch-wide-rows}
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
for r in json.load(open(sorted(files, key=os.path.getmtime)[-1])).get("results") or []:
    name = (r.get("name") or "").replace("test_storage_s3/test.py::", "")
    print(f"{label}\t{name}\t{r.get('status')}")
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

    # Reset the test file before appending, so that a checkout left over from an earlier run
    # picks up edits to the tests instead of keeping the copy it was first given.
    target="$root/tests/integration/test_storage_s3/test.py"
    git -C "$root" checkout -- tests/integration/test_storage_s3/test.py
    printf '\n\n%s' "$BLOCK" >> "$target"

    mkdir -p "$root/ci/tmp"
    if [ ! -s "$root/ci/tmp/clickhouse" ]; then
        cid=$(docker create "clickhouse/clickhouse-server:$version")
        docker cp "$cid:/usr/bin/clickhouse" "$root/ci/tmp/clickhouse"
        docker rm -f "$cid" > /dev/null
    fi

    docker ps -aq --filter name=praktika | xargs -r docker rm -f > /dev/null 2>&1 || true
    started_at=$(python3 -c 'import time; print(time.time())')
    run_status=0
    ( cd "$root" && python3 -u -m ci.praktika run integration --test "${TESTS[@]}" ) \
        > "$WORK/$version.log" 2>&1 || run_status=$?

    if grep -q "failed to start" "$WORK/$version.log"; then
        echo -e "$version\t(server did not start)\t-" >> "$RESULTS"
    else
        report "$version" "$root" "$started_at" >> "$RESULTS"
        # A non-zero runner status with no result rows means the run itself broke, which is
        # not the same as a test failing and must not be reported as a pass.
        if [ "$run_status" -ne 0 ] && ! grep -q "^$version	" "$RESULTS"; then
            echo -e "$version\t(runner exited $run_status, see $WORK/$version.log)\t-" >> "$RESULTS"
        fi
    fi
done

echo
printf '%-8s %-58s %s\n' VERSION TEST STATUS
while IFS=$'\t' read -r v t s; do printf '%-8s %-58s %s\n' "$v" "$t" "$s"; done < "$RESULTS"
