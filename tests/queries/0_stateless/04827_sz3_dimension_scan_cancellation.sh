#!/usr/bin/env bash
# Tags: long, no-fasttest, no-random-settings, no-random-merge-tree-settings
# no-fasttest: needs sz3 library
# no-random-settings: the test issues a single large controlled `INSERT` and terminates it via
# `KILL QUERY`; randomized query limits (`max_rows_to_read`, `max_execution_time`, ...) would abort it
# early, and a `max_compress_block_size` that is not a multiple of the float width is rejected by the
# SZ3 codec at write time.
# no-random-merge-tree-settings: the part must stay Wide with a fixed granule cadence.
#
# Codecs with `needsVectorDimensionUpfront` (SZ3) require an upfront pass over the whole column
# (`MergeTreeDataPartWriterOnDisk::setVectorDimensionsIfNeeded`) before the granule write loop. That
# pass materializes a `Field` per row, so on a large block of `Array(Float*)` it runs for a long time
# *before* the first per-granule cancellation check -- a `KILL`ed `INSERT` used to stay uninterruptible
# for the whole scan. The scan now observes cancellation per row; this test `KILL`s an `INSERT` right
# after the source is fully read (i.e. execution is inside the writer, starting with that scan) and
# requires the `KILL ... SYNC` to return within a bound and the `INSERT` to fail with
# `QUERY_WAS_CANCELLED`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROWS=4000000

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_sz3_cancel_src"

# The source rows are materialized once so reading them back is cheap and the measured INSERT below
# spends its time in the destination part writer (dimension scan + SZ3 compression). Random values
# keep the SZ3 quantizer on the slow unpredictable path, so the write phase lasts several seconds
# even on a fast release build.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_sz3_cancel_src (a Array(Float32)) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} --max_block_size $ROWS --max_insert_block_size $ROWS \
    -q "INSERT INTO t_sz3_cancel_src SELECT arrayMap(i -> randCanonical(i + number * 64)::Float32 * 1e6, range(64)) FROM numbers($ROWS)"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_sz3_cancel"
${CLICKHOUSE_CLIENT} --allow_experimental_codecs 1 -q "
CREATE TABLE t_sz3_cancel (a Array(Float32) CODEC(SZ3))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0
"

query_id="sz3_dim_scan_cancel_${CLICKHOUSE_DATABASE}_$$"
err="${CLICKHOUSE_TMP}/04827_sz3_cancel_err.txt"

# One squashed insert block, so the writer performs a single long dimension scan over all rows.
${CLICKHOUSE_CLIENT} --query_id "$query_id" \
    --max_block_size $ROWS --max_insert_block_size $ROWS \
    --min_insert_block_size_rows $ROWS --min_insert_block_size_bytes 0 \
    -q "INSERT INTO t_sz3_cancel SELECT a FROM t_sz3_cancel_src" >/dev/null 2>"$err" &
insert_pid=$!

# Deterministic phase signal: once all source rows are read, the block is squashed and execution is
# inside the destination part writer -- which for an SZ3 column starts with the dimension scan.
read_rows=0
for _ in $(seq 1 600); do
    read_rows=$(${CLICKHOUSE_CLIENT} -q "SELECT read_rows FROM system.processes WHERE query_id = '$query_id'")
    if [ -n "$read_rows" ] && [ "$read_rows" -ge "$ROWS" ]; then break; fi
    sleep 0.1
done

if [ -z "$read_rows" ] || [ "$read_rows" -lt "$ROWS" ]; then
    echo "did not observe the write phase"
    cat "$err"
    timeout 15 ${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null" >/dev/null || true
    kill "$insert_pid" 2>/dev/null
    wait "$insert_pid" 2>/dev/null
# With the per-row check the kill is observed within microseconds wherever the writer is (dimension
# scan or granule loop). Without it, the KILL blocks for the remainder of the whole-column scan, which
# on slow (debug/sanitizer) builds exceeds this bound.
elif timeout 15 ${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null"
then
    wait "$insert_pid" 2>/dev/null
    if grep -q "QUERY_WAS_CANCELLED" "$err"; then
        echo "killed promptly"
    else
        echo "insert was not cancelled"
        cat "$err"
    fi
else
    echo "KILL QUERY SYNC did not return in time"
    kill "$insert_pid" 2>/dev/null
    wait "$insert_pid" 2>/dev/null
fi

rm -f "$err"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_sz3_cancel"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_sz3_cancel_src"
