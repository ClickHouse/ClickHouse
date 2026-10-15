#!/usr/bin/env bash
# Tags: no-fasttest
# Verifies that `engine_file_skip_failed_data_files=1` does NOT swallow fatal errors
# such as `MEMORY_LIMIT_EXCEEDED` - those must propagate to the user.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=${CLICKHOUSE_USER_FILES_UNIQUE:?}

cleanup() {
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

mkdir -p "$DATA_DIR"

# Create a non-trivial valid gzip file so reading it actually allocates memory.
yes "some payload row" | head -n 100000 | gzip -c > "$DATA_DIR/payload.gz"

# With a tiny memory limit and the skip setting on, the query MUST still fail:
# if the setting wrongly swallowed the OOM error, the file would be "skipped" and
# the query would succeed with 0 rows. The exact error surfaced to the client can
# vary (the exception may fire mid-packet under aggressive memory profiling), so
# assert failure rather than a specific error code.
if $CLICKHOUSE_CLIENT -q "SELECT length(groupArray(line)) FROM file('$DATA_DIR/payload.gz', 'LineAsString', 'line String') SETTINGS engine_file_skip_failed_data_files=1, max_memory_usage=10000, max_untracked_memory=1, memory_profiler_step=1" >/dev/null 2>&1; then
    echo "query unexpectedly succeeded"
else
    echo "1"
fi
