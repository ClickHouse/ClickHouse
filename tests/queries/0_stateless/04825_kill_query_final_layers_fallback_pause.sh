#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-sanitizers-lsan
# Test that KILL QUERY works when the kill lands in the fallback full-chunk evaluation path of
# `FilterSortedStreamByRange` (the quick two-row probe found rows that do not pass the range filter,
# so the whole chunk has to be filtered by the inner `FilterTransform`). After the kill the outer
# transform must stop pulling further input: the `filter_sorted_stream_by_range_pause` failpoint is
# armed only after the kill, so any transform entry after the cancellation would block on it forever
# and the query would never finish.
# no-parallel: the failpoints are global, unrelated queries could consume them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The trap is installed before any server state is changed, so the setup (stopped merges, the table,
# the failpoints) is unwound even when one of the guard assertions below fails and exits early.
trap '${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT filter_sorted_stream_by_range_fallback_pause" 2>/dev/null; ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT filter_sorted_stream_by_range_pause" 2>/dev/null; ${CLICKHOUSE_CLIENT} -q "SYSTEM START MERGES t_final_layers_fallback" 2>/dev/null; ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_final_layers_fallback" 2>/dev/null' EXIT

# The layered plan exists only while the two overlapping parts are separate: a background merge would
# collapse them and silently turn the test into a no-op (or make the failpoint wait hang), so merges are
# stopped for the table before the parts are created and only restarted at the end.
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_final_layers_fallback;
    CREATE TABLE t_final_layers_fallback (a UInt64, b UInt64) ENGINE = ReplacingMergeTree ORDER BY a;
    SYSTEM STOP MERGES t_final_layers_fallback;
    SET optimize_on_insert = 0;
    INSERT INTO t_final_layers_fallback SELECT number, number FROM numbers(100000);
    INSERT INTO t_final_layers_fallback SELECT number, number FROM numbers(50000, 100000);
"

# The plan must actually contain the processor under test, otherwise the test silently degrades to a no-op.
${CLICKHOUSE_CLIENT} -q "
    EXPLAIN PIPELINE SELECT count() FROM t_final_layers_fallback FINAL
    SETTINGS max_threads = 4, enable_vertical_final = 0, split_intersecting_parts_ranges_into_layers_final = 1
" | grep -qF "FilterSortedStreamByRange" || { echo "FAIL: no FilterSortedStreamByRange in the pipeline"; exit 1; }

query_id="kill_query_final_layers_fallback_${CLICKHOUSE_DATABASE}_$RANDOM"
output_file="${CLICKHOUSE_TMP}/kill_query_final_layers_fallback_${CLICKHOUSE_DATABASE}.out"

# Pause the first stream whose two-row probe fails, right before the fallback full-chunk evaluation.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT filter_sorted_stream_by_range_fallback_pause"

# The layer border cuts through the rows of both parts, so the chunks that straddle the border fail
# the quick probe and take the fallback path. The client is timeout-bounded: if a regression makes the
# killed query unkillable (blocked on the re-entry failpoint below), the test must fail instead of hang.
timeout 60 ${CLICKHOUSE_CLIENT} --query_id="$query_id" --query "
    SELECT count() FROM t_final_layers_fallback FINAL
    FORMAT Null
    SETTINGS max_threads = 4, enable_vertical_final = 0, split_intersecting_parts_ranges_into_layers_final = 1
" >"$output_file" 2>&1 &

# Bounded, so that a plan-shape change that stops producing fallback evaluations fails the test cleanly.
timeout 60 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT filter_sorted_stream_by_range_fallback_pause PAUSE" || {
    echo "FAIL: the fallback full-chunk evaluation path was never reached"
    ${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '$query_id' ASYNC" >/dev/null
    exit 1
}

# Kill the query while one stream is held right before the fallback evaluation.
${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null

# Only now arm the re-entry trap: every processor of the query is already cancelled, so with the
# cancellation guards in place nothing reaches it and the query finishes. Without the guards the outer
# transform pulls the next buffered chunk after the kill, blocks here, and the client times out above.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT filter_sorted_stream_by_range_pause"

# Release the paused stream - it must observe the cancellation and stop instead of continuing to read.
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT filter_sorted_stream_by_range_fallback_pause"

wait

grep -qF "QUERY_WAS_CANCELLED" "$output_file" || { echo "FAIL: query was not cancelled"; cat "$output_file"; exit 1; }

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT filter_sorted_stream_by_range_pause"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_final_layers_fallback"

echo "OK"
