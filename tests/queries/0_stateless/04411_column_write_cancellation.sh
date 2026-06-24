#!/usr/bin/env bash
# Tags: long, no-random-merge-tree-settings
#
# A `KILL`ed `INSERT` must abort while the part writer is serializing column data, not only between
# blocks. We write a single large block of high-entropy strings with a slow `ZSTD(22)` codec so
# column serialization (`MergeTreeDataPartWriterWide::writeColumn`) is the active, multi-second phase,
# then cancel it. Without the in-loop cancellation check the `KILL` blocks until the whole block is
# written and the bounded `KILL QUERY` below trips its timeout.
#
# no-random-merge-tree-settings: a randomized index_granularity changes the granule/throttle cadence
# and the write cost assumptions here.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_col_write_cancel"
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_col_write_cancel (s String CODEC(ZSTD(22)))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0
"

query_id="col_write_cancel_${CLICKHOUSE_DATABASE}_$$"
err="${CLICKHOUSE_TMP}/04411_col_write_err.txt"

# Single large block of incompressible data: ZSTD(22) makes the column write CPU-bound for many
# seconds while staying modest in memory.
${CLICKHOUSE_CLIENT} --query_id "$query_id" \
    --max_block_size 4000000 --max_insert_block_size 4000000 \
    --min_insert_block_size_rows 0 --min_insert_block_size_bytes 0 \
    -q "INSERT INTO t_col_write_cancel SELECT randomString(64) FROM numbers(4000000)" >/dev/null 2>"$err" &
insert_pid=$!

# Wait until the query is running and past source generation (so the cancel has to be observed inside
# the column write loop rather than between blocks).
elapsed=0
for _ in $(seq 1 600); do
    elapsed=$(${CLICKHOUSE_CLIENT} -q "SELECT max(elapsed) FROM system.processes WHERE query_id = '$query_id'")
    if [ -n "$elapsed" ] && awk "BEGIN{exit !($elapsed >= 2)}"; then break; fi
    sleep 0.1
done

if ! awk "BEGIN{exit !($elapsed >= 2)}" 2>/dev/null; then
    echo "did not observe the column write phase"
    cat "$err"
# On the fixed server the cancel is observed at the next throttled check and `KILL QUERY` returns
# quickly; bound it so a regression (cancel ignored mid-write) fails the test instead of hanging.
elif timeout 5 ${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null"
then
    # KILL returned in time, but that alone doesn't prove the write was interrupted: confirm the
    # background INSERT actually failed with QUERY_WAS_CANCELLED (not that it just finished naturally).
    wait "$insert_pid" 2>/dev/null
    if grep -q "QUERY_WAS_CANCELLED" "$err"; then
        echo "killed promptly"
    else
        echo "insert was not cancelled"
        cat "$err"
    fi
else
    echo "KILL QUERY SYNC did not return in time"
    # Still spinning on a regression; terminate the client so the test finishes bounded.
    kill "$insert_pid" 2>/dev/null
    wait "$insert_pid" 2>/dev/null
fi

rm -f "$err"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_col_write_cancel"
