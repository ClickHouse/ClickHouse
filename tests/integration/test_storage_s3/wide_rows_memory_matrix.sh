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
    python3 - "$1" "$2" <<'PY'
import glob, json, os, sys
label, root = sys.argv[1], sys.argv[2]
files = sorted(glob.glob(os.path.join(root, "ci/tmp/result_integration_tests_*.json")), key=os.path.getmtime)
if not files:
    print(f"{label}\t(no result file)\t-")
    raise SystemExit
for r in json.load(open(files[-1])).get("results") or []:
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

    target="$root/tests/integration/test_storage_s3/test.py"
    if ! grep -q "_write_wide_parquet" "$target"; then
        printf '\n\n%s' "$BLOCK" >> "$target"
    fi

    mkdir -p "$root/ci/tmp"
    if [ ! -s "$root/ci/tmp/clickhouse" ]; then
        cid=$(docker create "clickhouse/clickhouse-server:$version")
        docker cp "$cid:/usr/bin/clickhouse" "$root/ci/tmp/clickhouse"
        docker rm -f "$cid" > /dev/null
    fi

    docker ps -aq --filter name=praktika | xargs -r docker rm -f > /dev/null 2>&1 || true
    ( cd "$root" && python3 -u -m ci.praktika run integration --test "${TESTS[@]}" ) \
        > "$WORK/$version.log" 2>&1 || true

    if grep -q "failed to start" "$WORK/$version.log"; then
        echo -e "$version\t(server did not start)\t-" >> "$RESULTS"
    else
        report "$version" "$root" >> "$RESULTS"
    fi
done

echo
printf '%-8s %-58s %s\n' VERSION TEST STATUS
while IFS=$'\t' read -r v t s; do printf '%-8s %-58s %s\n' "$v" "$t" "$s"; done < "$RESULTS"
