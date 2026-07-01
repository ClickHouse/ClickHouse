#!/usr/bin/env bash
# Tags: long, no-random-settings, no-random-merge-tree-settings
#
# A `KILL`ed `INSERT` must abort while the part writer is serializing a single large block, not only
# between blocks. The check the PR adds lives in the shared writer base and is called from both the
# Wide (`MergeTreeDataPartWriterWide::writeColumn`) and Compact
# (`MergeTreeDataPartWriterCompact::writeDataBlock`) column-write loops, so this test exercises BOTH:
# it runs the same cancellation scenario against a forced-Wide destination and a forced-Compact one.
#
# The high-entropy source rows are materialized first into a plain table, so the measured
# `INSERT ... SELECT` is dominated by the slow `ZSTD(22)` column serialization rather than by
# `randomString` generation, and `min_insert_block_size_rows` forces the whole input into one insert
# block so the writer loops over many granules inside a single write call -- the only place the new
# per-granule check can interrupt (a small block would already be cancellable between blocks). We wait
# until the source is fully read and squashed (so execution is inside that one big write) and then
# cancel. Without the in-loop cancellation check the `KILL` blocks until the whole block is written and
# the bounded `KILL QUERY` below trips its timeout.
#
# no-random-settings: the test issues a single large controlled `INSERT` and manages termination via
# `KILL QUERY`; randomized query limits would break that -- e.g. a low `max_rows_to_read` /
# `max_memory_usage` aborts it early, and a random `max_execution_time` would terminate it instead of
# our `KILL`. We need the read/time/memory limits left at their (unlimited) defaults.
# no-random-merge-tree-settings: a randomized index_granularity changes the granule/throttle cadence
# and the write cost assumptions here.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROWS=4000000

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_col_write_src"

# Source rows are materialized once (no slow codec) so reading them back is cheap; this guarantees the
# measured INSERT below spends its time in column serialization, not in randomString generation.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_col_write_src (s String) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} --max_block_size $ROWS --max_insert_block_size $ROWS \
    -q "INSERT INTO t_col_write_src SELECT randomString(64) FROM numbers($ROWS)"

# Run the cancel-during-write scenario against one destination part kind ($1 = label, $2 = the
# wide/compact-forcing settings) and print "<label>: <result>".
run_case()
{
    local label="$1"
    local part_settings="$2"
    local table="t_col_write_cancel_${label}"

    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${table}"
    # Slow ZSTD(22) codec keeps column write CPU-bound for many seconds while staying modest in memory.
    ${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE ${table} (s String CODEC(ZSTD(22)))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS index_granularity = 8192, ${part_settings}
    "

    local query_id="col_write_cancel_${label}_${CLICKHOUSE_DATABASE}_$$"
    local err="${CLICKHOUSE_TMP}/04411_col_write_err_${label}.txt"

    # Single large block read from the pre-materialized source, squashed into one insert block, then
    # serialized with ZSTD(22).
    ${CLICKHOUSE_CLIENT} --query_id "$query_id" \
        --max_block_size $ROWS --max_insert_block_size $ROWS \
        --min_insert_block_size_rows $ROWS --min_insert_block_size_bytes 0 \
        -q "INSERT INTO ${table} SELECT s FROM t_col_write_src" >/dev/null 2>"$err" &
    local insert_pid=$!

    # Deterministic phase signal: once all source rows are read (read_rows == ROWS), the source is
    # exhausted and squashed into the single insert block, so execution is inside the destination part
    # writer serializing that block. Cancelling here forces the cancel to be observed in the column
    # write loop, not in source generation / between blocks.
    local read_rows=0
    local _
    for _ in $(seq 1 600); do
        read_rows=$(${CLICKHOUSE_CLIENT} -q "SELECT read_rows FROM system.processes WHERE query_id = '$query_id'")
        if [ -n "$read_rows" ] && [ "$read_rows" -ge "$ROWS" ]; then break; fi
        sleep 0.1
    done

    if [ -z "$read_rows" ] || [ "$read_rows" -lt "$ROWS" ]; then
        echo "${label}: did not observe the column write phase"
        cat "$err"
    # On the fixed server the cancel is observed at the next throttled check (within one 65536-row
    # batch, well under a second even on sanitizer builds) and `KILL QUERY` returns quickly. The bound
    # is far below the full-block ZSTD(22) write time (tens of seconds), so a regression -- KILL ignored
    # until the whole block is written -- still trips the timeout instead of hanging.
    elif timeout 15 ${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null"
    then
        # KILL returned in time, but that alone doesn't prove the write was interrupted: confirm the
        # background INSERT actually failed with QUERY_WAS_CANCELLED (not that it just finished).
        wait "$insert_pid" 2>/dev/null
        if grep -q "QUERY_WAS_CANCELLED" "$err"; then
            echo "${label}: killed promptly"
        else
            echo "${label}: insert was not cancelled"
            cat "$err"
        fi
    else
        echo "${label}: KILL QUERY SYNC did not return in time"
        # Still spinning on a regression; terminate the client so the test finishes bounded.
        kill "$insert_pid" 2>/dev/null
        wait "$insert_pid" 2>/dev/null
    fi

    rm -f "$err"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE ${table}"
}

# Wide: writeColumn loop. Compact: writeDataBlock loop. The thresholds force the 4M-row part to the
# chosen kind (both thresholds must sit above the part size to keep it Compact).
run_case wide    "min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0"
run_case compact "min_bytes_for_wide_part = '100G', min_rows_for_wide_part = 1000000000"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_col_write_src"
