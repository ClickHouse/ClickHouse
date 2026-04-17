#!/usr/bin/env bash
# Tags: no-fasttest
# Verifies that engine_file_skip_failed_data_files=1 does NOT swallow fatal errors
# such as MEMORY_LIMIT_EXCEEDED - those must propagate to the user.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

if ! command -v gzip &> /dev/null; then echo "gzip not found" 1>&2; exit 1; fi

TEST_DIR_NAME=test_04141_fatal
DATA_DIR=${USER_FILES_PATH:?}/$TEST_DIR_NAME

cleanup() {
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

mkdir -p "$DATA_DIR"

# Create a non-trivial valid gzip file so reading it actually allocates memory.
yes "some payload row" | head -n 100000 > "$DATA_DIR/payload.txt"
gzip -c "$DATA_DIR/payload.txt" > "$DATA_DIR/payload.gz"
rm "$DATA_DIR/payload.txt"

# With a tiny memory limit and the skip setting on, the query MUST still fail
# with MEMORY_LIMIT_EXCEEDED (or related OOM code) - we expect 1 match.
$CLICKHOUSE_CLIENT -q "SELECT length(groupArray(line)) FROM file('$DATA_DIR/payload.gz', 'LineAsString', 'line String') SETTINGS engine_file_skip_failed_data_files=1, max_memory_usage=10000, max_untracked_memory=1, memory_profiler_step=1" 2>&1 \
    | grep -q -E "MEMORY_LIMIT_EXCEEDED|CANNOT_ALLOCATE_MEMORY" && echo "1" || echo "0"
