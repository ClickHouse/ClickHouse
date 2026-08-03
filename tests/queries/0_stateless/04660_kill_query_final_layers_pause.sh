#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-sanitizers-lsan
# Test that KILL QUERY works for the layered `FINAL` read plan, where the range filter of every layer
# is applied by a `FilterSortedStreamByRange` that owns an inner `FilterTransform`.
# The pipeline cancels only the outer processor, so `FilterSortedStreamByRange::onCancel` has to forward
# the cancellation into the inner transform. The `filter_transform_pause` failpoint stops the query inside
# that inner transform; then the query is killed and the cancellation has to be detected.
# no-parallel: filter_transform_pause is a global PAUSEABLE_ONCE failpoint, unrelated queries could consume it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The layered plan exists only while the two overlapping parts are separate: a background merge would
# collapse them and silently turn the test into a no-op (or make the failpoint wait hang), so merges are
# stopped for the table before the parts are created and only restarted at the end.
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_final_layers;
    CREATE TABLE t_final_layers (a UInt64, b UInt64) ENGINE = ReplacingMergeTree ORDER BY a;
    SYSTEM STOP MERGES t_final_layers;
    SET optimize_on_insert = 0;
    INSERT INTO t_final_layers SELECT number, number FROM numbers(100000);
    INSERT INTO t_final_layers SELECT number, number FROM numbers(50000, 100000);
"

# The plan must actually contain the processor under test, otherwise the test silently degrades to a no-op.
${CLICKHOUSE_CLIENT} -q "
    EXPLAIN PIPELINE SELECT count() FROM t_final_layers FINAL
    SETTINGS max_threads = 4, enable_vertical_final = 0, split_intersecting_parts_ranges_into_layers_final = 1
" | grep -qF "FilterSortedStreamByRange" || { echo "FAIL: no FilterSortedStreamByRange in the pipeline"; exit 1; }

query_id="kill_query_final_layers_pause_${CLICKHOUSE_DATABASE}_$RANDOM"
output_file="${CLICKHOUSE_TMP}/kill_query_final_layers_pause_${CLICKHOUSE_DATABASE}.out"

trap '${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT filter_transform_pause" 2>/dev/null; ${CLICKHOUSE_CLIENT} -q "SYSTEM START MERGES t_final_layers" 2>/dev/null' EXIT

# Enable the failpoint before starting the query
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT filter_transform_pause"

# Start the layered FINAL query; it will pause inside the inner FilterTransform of FilterSortedStreamByRange
${CLICKHOUSE_CLIENT} --query_id="$query_id" --query "
    SELECT count() FROM t_final_layers FINAL
    FORMAT Null
    SETTINGS max_threads = 4, enable_vertical_final = 0, split_intersecting_parts_ranges_into_layers_final = 1
" >"$output_file" 2>&1 &

${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT filter_transform_pause PAUSE"

# Kill the query (ASYNC) - this cancels the outer FilterSortedStreamByRange, which forwards the cancellation
${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null

# Release the failpoint - the inner transform should observe the cancellation
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT filter_transform_pause"

wait

grep -qF "QUERY_WAS_CANCELLED" "$output_file" || { echo "FAIL: query was not cancelled"; cat "$output_file"; exit 1; }

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_final_layers"

echo "OK"
