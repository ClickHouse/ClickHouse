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
# A final case checks that a throw-mode `max_execution_time` expiring inside the same write loop is
# reported as `TIMEOUT_EXCEEDED` (not `QUERY_WAS_CANCELLED`), i.e. the writer-side check preserves the
# recorded cancel reason.
#
# no-random-settings: the test issues a single large controlled `INSERT` and manages termination via
# `KILL QUERY`; randomized query limits would break that -- e.g. a low `max_rows_to_read` /
# `max_memory_usage` aborts it early, and a random `max_execution_time` would terminate it instead of
# our `KILL`. We need the read/time/memory limits left at their (unlimited) defaults.
# no-random-merge-tree-settings: a randomized index_granularity changes the granule cadence and the
# write cost assumptions here.

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
        # The INSERT may still be running (e.g. a stuck read); terminate it bounded so this failure
        # stays a clean FAIL instead of leaving a client behind for the harness hung-check.
        timeout 15 ${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null" >/dev/null || true
        kill "$insert_pid" 2>/dev/null
        wait "$insert_pid" 2>/dev/null
    # On the fixed server the cancel is observed at the next granule boundary (well under a second even
    # on sanitizer builds) and `KILL QUERY` returns quickly. The bound
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

# A throw-mode `max_execution_time` that expires while the writer is serializing the block must report
# `TIMEOUT_EXCEEDED`, not `QUERY_WAS_CANCELLED`. Both a KILL and a throw-mode timeout set the same
# `is_killed` flag (the timeout via `CancellationChecker::cancelQuery(CancelReason::TIMEOUT)`), so the
# writer-side check calls `QueryStatus::throwIfKilled`, which maps the recorded cancel reason back to
# the right error code. This case guards that mapping for the write path (the exception mapping lives in
# the shared writer base, so exercising one part kind covers both).
run_timeout_case()
{
    local table="t_col_write_timeout_wide"

    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${table}"
    ${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE ${table} (s String CODEC(ZSTD(22)))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0
    "

    local query_id="col_write_timeout_${CLICKHOUSE_DATABASE}_$$"
    local err="${CLICKHOUSE_TMP}/04411_col_write_timeout_err.txt"

    # `max_execution_time = 8` (throw) is far above the cheap read of the pre-materialized source (a few
    # seconds at most, even on sanitizer builds) and far below the ZSTD(22) write of the whole block
    # (tens of seconds to minutes), so the deadline reliably lands inside the column write loop.
    ${CLICKHOUSE_CLIENT} --query_id "$query_id" \
        --max_block_size $ROWS --max_insert_block_size $ROWS \
        --min_insert_block_size_rows $ROWS --min_insert_block_size_bytes 0 \
        --max_execution_time 8 --timeout_overflow_mode throw \
        -q "INSERT INTO ${table} SELECT s FROM t_col_write_src" >/dev/null 2>"$err" &
    local insert_pid=$!

    # Confirm the query reaches the write phase (source fully read => execution inside the writer) before
    # the deadline; this is what makes the timeout fire in the column write loop rather than in the read.
    local read_rows
    local max_read_rows=0
    local _
    for _ in $(seq 1 600); do
        read_rows=$(${CLICKHOUSE_CLIENT} -q "SELECT read_rows FROM system.processes WHERE query_id = '$query_id'")
        if [ -n "$read_rows" ] && [ "$read_rows" -gt "$max_read_rows" ]; then max_read_rows=$read_rows; fi
        if [ "$max_read_rows" -ge "$ROWS" ]; then break; fi
        # Stop polling once the query is gone (already finished / timed out).
        if ! kill -0 "$insert_pid" 2>/dev/null; then break; fi
        sleep 0.1
    done

    # Bound the wait for the 8-second deadline: `CancellationChecker` can miss it entirely (its worker
    # sleeps toward a stale earliest deadline and a newly appended earlier one does not re-arm the wait),
    # and then the untracked INSERT would grind through the full ZSTD(22) block and trip the harness
    # timeout instead of this test. Kill the query and fail with a diagnostic in that case.
    local deadline_fired=0
    for _ in $(seq 1 600); do
        if ! kill -0 "$insert_pid" 2>/dev/null; then deadline_fired=1; break; fi
        sleep 0.1
    done
    if [ "$deadline_fired" -eq 0 ]; then
        echo "timeout wide: max_execution_time never fired within 60s, killing the query"
        # Bounded like the KILL in run_case: if the writer-side check regressed too, an unbounded
        # SYNC would block until the whole block is written; fall back to killing the client.
        if ! timeout 15 ${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null" >/dev/null; then
            kill "$insert_pid" 2>/dev/null
        fi
    fi

    wait "$insert_pid" 2>/dev/null

    # The query may exit between polls, so a low in-flight maximum is not yet proof that the read phase
    # ate the deadline: re-read the final counter from the log before judging.
    if [ "$max_read_rows" -lt "$ROWS" ]; then
        ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS system.query_log"
        max_read_rows=$(${CLICKHOUSE_CLIENT} -q "SELECT coalesce(max(read_rows), 0) FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '$query_id'")
    fi

    # A TIMEOUT_EXCEEDED alone is not enough: if the source was never fully read, the deadline fired in
    # the read/squash phase and the writer-side check was never exercised.
    if [ -z "$max_read_rows" ] || [ "$max_read_rows" -lt "$ROWS" ]; then
        echo "timeout wide: did not observe the column write phase"
        cat "$err"
    elif grep -q "TIMEOUT_EXCEEDED" "$err"; then
        echo "timeout wide: reported as timeout"
    else
        echo "timeout wide: wrong error"
        cat "$err"
    fi

    rm -f "$err"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE ${table}"
}

run_timeout_case

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_col_write_src"
