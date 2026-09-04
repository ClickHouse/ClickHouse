#!/usr/bin/env bash
# Tags: long, no-random-settings, no-random-merge-tree-settings
#
# Regression test for the *byte-heavy* shape of writer-side cancellation: a block whose cost is
# dominated by bytes and codec work rather than by the number of rows. The whole `INSERT` here is only
# 8192 rows -- far fewer than the 65536-row batch an earlier version of the check accumulated before
# looking at the cancellation flag -- but each row is 32 KiB, so serializing the block with `ZSTD(22)`
# takes tens of seconds. With a row-count throttle such a write is never interruptible at all; the
# cancellation check must therefore run for every granule (a plain atomic load), which is what this
# test pins down.
#
# `index_granularity_bytes` is what splits this block into many granules (~32 rows each), since the
# 8192 rows would otherwise form a single granule at the default `index_granularity`.
#
# no-random-settings: the `INSERT` is terminated by an explicit `KILL QUERY`; a randomized
# `max_execution_time` / `max_memory_usage` / `max_rows_to_read` would end it on its own instead.
# no-random-merge-tree-settings: the granule layout (`index_granularity`, `index_granularity_bytes`)
# and the wide/compact choice are exactly what this test controls.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROWS=8192
ROW_BYTES=32768

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_wide_rows_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_wide_rows_dst"

# Materialize the wide source rows first (cheap default codec), so the measured INSERT below spends its
# time in `ZSTD(22)` column serialization rather than in `randomString` generation.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_wide_rows_src (s String) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} --max_block_size $ROWS --max_insert_block_size $ROWS \
    -q "INSERT INTO t_wide_rows_src SELECT randomString($ROW_BYTES) FROM numbers($ROWS)"

${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_wide_rows_dst (s String CODEC(ZSTD(22)))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, index_granularity_bytes = 1048576,
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0
"

query_id="col_write_wide_rows_${CLICKHOUSE_DATABASE}_$$"
err="${CLICKHOUSE_TMP}/04661_col_write_wide_rows_err.txt"

# One insert block holding all 8192 rows, so the writer loops over the granules of that block inside a
# single write call -- the only place the writer-side check can interrupt.
${CLICKHOUSE_CLIENT} --query_id "$query_id" \
    --max_block_size $ROWS --max_insert_block_size $ROWS \
    --min_insert_block_size_rows $ROWS --min_insert_block_size_bytes 0 \
    -q "INSERT INTO t_wide_rows_dst SELECT s FROM t_wide_rows_src" >/dev/null 2>"$err" &
insert_pid=$!

# Once all source rows are read, the source is exhausted and squashed into the single insert block, so
# execution is inside the destination part writer serializing that block.
read_rows=0
for _ in $(seq 1 600); do
    read_rows=$(${CLICKHOUSE_CLIENT} -q "SELECT read_rows FROM system.processes WHERE query_id = '$query_id'")
    if [ -n "$read_rows" ] && [ "$read_rows" -ge "$ROWS" ]; then break; fi
    sleep 0.1
done

if [ -z "$read_rows" ] || [ "$read_rows" -lt "$ROWS" ]; then
    echo "wide rows: did not observe the column write phase"
    cat "$err"
    timeout 15 ${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null" >/dev/null || true
    kill "$insert_pid" 2>/dev/null
    wait "$insert_pid" 2>/dev/null
# The bound is far below the full-block `ZSTD(22)` write time, so a regression -- the cancellation flag
# observed only after a row-count batch that this block never reaches -- trips the timeout.
elif timeout 15 ${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null"
then
    wait "$insert_pid" 2>/dev/null
    if grep -q "QUERY_WAS_CANCELLED" "$err"; then
        echo "wide rows: killed promptly"
    else
        echo "wide rows: insert was not cancelled"
        cat "$err"
    fi
else
    echo "wide rows: KILL QUERY SYNC did not return in time"
    kill "$insert_pid" 2>/dev/null
    wait "$insert_pid" 2>/dev/null
fi

rm -f "$err"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_wide_rows_dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_wide_rows_src"
