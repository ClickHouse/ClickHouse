#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-sanitizers-lsan, long
# Test that KILL QUERY with use_query_condition_cache does not seed the QueryConditionCache.
# Uses a MergeTree table (produces MarkRangesInfo for the cache) and the
# filter_transform_pause failpoint to stop the query after expression execution but
# before the cache write in doTransform.
# Verifies:
#   1. Cache count is unchanged after a cancelled query.
#   2. Cache count grows after a successful query with the same filter.
# no-parallel: filter_transform_pause is a global PAUSEABLE_ONCE failpoint.
# no-parallel: query_condition_cache is instance-wide, and a parallel table drop clears it.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

query_id="kill_query_filter_cache_${CLICKHOUSE_DATABASE}_$RANDOM"
output_file="${CLICKHOUSE_TMP}/kill_query_filter_cache_${CLICKHOUSE_DATABASE}.out"

trap '${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT filter_transform_pause" 2>/dev/null; ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS 04614_t" 2>/dev/null' EXIT

# Create a MergeTree table (produces MarkRangesInfo, needed by QueryConditionCache)
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS 04614_t"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE 04614_t (t UInt64, v UInt64) ENGINE = MergeTree ORDER BY t"
${CLICKHOUSE_CLIENT} -q "INSERT INTO 04614_t SELECT number, number FROM numbers(1000000)"

# Record cache baseline before any query uses use_query_condition_cache
cache_before=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.query_condition_cache")

# Enable failpoint before starting the query
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT filter_transform_pause"

# Start a filter query with use_query_condition_cache=1 that will pause at the failpoint.
# WHERE v > 999999 rejects all rows (num_filtered_rows == 0 for every block),
# which would normally trigger a cache write in doTransform.
${CLICKHOUSE_CLIENT} --query_id="$query_id" --query "
    SELECT count()
    FROM 04614_t
    WHERE v > 999999
    FORMAT Null
    SETTINGS use_query_condition_cache = 1, max_block_size=100000, max_threads=1, max_rows_to_read=0
" >"$output_file" 2>&1 &

# Wait for the failpoint to be hit (query is now blocked in doTransform after expression execution)
${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT filter_transform_pause PAUSE"

# Kill the query (ASYNC) — onCancel cancels functions and sets isCancelled
${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null

# Disable failpoint — query sees isCancelled() and returns early without writing to cache
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT filter_transform_pause"

wait

# Assert cancellation was detected, not normal completion
grep -qF "QUERY_WAS_CANCELLED" "$output_file" || { echo "FAIL: query was not cancelled"; exit 1; }

# Cache should NOT have been seeded after cancellation
cache_after=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.query_condition_cache")
[[ "$cache_after" == "$cache_before" ]] || { echo "FAIL: cache count changed after cancelled query (before=$cache_before, after=$cache_after)"; exit 1; }

# Now run the same query successfully — it should populate the cache via doTransform + prepare flush
${CLICKHOUSE_CLIENT} -q "
    SELECT count()
    FROM 04614_t
    WHERE v > 999999
    FORMAT Null
    SETTINGS use_query_condition_cache = 1, max_block_size=100000, max_threads=1, max_rows_to_read=0
"

# Cache should have grown after successful query
cache_final=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.query_condition_cache")
[[ "$cache_final" -gt "$cache_before" ]] || { echo "FAIL: cache was not populated after successful query (before=$cache_before, final=$cache_final)"; exit 1; }

${CLICKHOUSE_CLIENT} -q "DROP TABLE 04614_t"

echo "OK"
