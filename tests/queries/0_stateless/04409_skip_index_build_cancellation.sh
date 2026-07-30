#!/usr/bin/env bash
# Tags: long, no-random-settings, no-random-merge-tree-settings
#
# A KILLed INSERT must abort an in-progress skip-index build promptly, instead of
# finishing the whole build first. We start a single-block INSERT that spends many
# seconds building an unbounded set(0) index, cancel it, and require KILL QUERY ... SYNC
# to return quickly. Without the per-granule cancellation check the KILL blocks until the
# whole block's build completes and the timeout below trips.
#
# The test reads 30M rows in a single large INSERT and controls termination itself via
# KILL QUERY, so the read/time limits must be left out of its way:
# * The CI 'default' test profile (tests/config/users.d/limits.yaml) caps 'max_rows_to_read'
#   at 20M, which aborts the 30M-row INSERT with TOO_MANY_ROWS before the build even starts.
#   We override it with --max_rows_to_read 0 on the INSERT below (this is a fixed profile
#   limit, not a randomized one, so no-random-settings alone does not relax it).
# * no-random-settings: a random 'max_execution_time' / 'max_memory_usage' would terminate
#   the long INSERT instead of our KILL.
# * no-random-merge-tree-settings: a randomized index_granularity changes the granule count
#   and can make the build pathologically slow or memory-heavy; the timing/memory assumptions
#   here need the pinned granularity below.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_skip_index_cancel"
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_skip_index_cancel
(
    x UInt64,
    INDEX idx x TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192
"

query_id="skip_index_cancel_${CLICKHOUSE_DATABASE}_$$"
insert_err="${CLICKHOUSE_TMP}/04409_insert_err.txt"

# Single large block of low-cardinality values: the set stays tiny (low memory) but the
# per-granule build loop runs for many seconds, and is the active phase when we cancel.
${CLICKHOUSE_CLIENT} --query_id "$query_id" \
    --max_block_size 30000000 --max_insert_block_size 30000000 \
    --min_insert_block_size_rows 0 --min_insert_block_size_bytes 0 \
    --max_rows_to_read 0 \
    -q "INSERT INTO t_skip_index_cancel SELECT number % 1000 FROM numbers(30000000)" >/dev/null 2>"$insert_err" &
insert_pid=$!

# Wait until the INSERT is actually building skip indices (past source generation), so the
# cancellation has to be observed inside the build loop rather than between blocks.
building=0
for _ in $(seq 1 600); do
    building=$(${CLICKHOUSE_CLIENT} -q "
        SELECT ProfileEvents['MergeTreeDataWriterSkipIndicesCalculationMicroseconds'] > 0
        FROM system.processes WHERE query_id = '$query_id'")
    [ "$building" = "1" ] && break
    sleep 0.1
done

if [ "$building" != "1" ]; then
    # Fail loudly instead of cancelling a query that never reached the skip-index build.
    echo "did not observe the skip-index build phase"
    echo "--- INSERT client output ---"
    cat "$insert_err"
# On the fixed server the cancel is observed at the next granule and KILL returns in well under a
# second; bound it so a regression (KILL ignored until the build finishes) fails instead of hanging.
elif timeout 10 ${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null"
then
    echo "killed promptly"
else
    echo "KILL QUERY SYNC did not return in time"
fi

wait "$insert_pid" 2>/dev/null

rm -f "$insert_err"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_skip_index_cancel"
