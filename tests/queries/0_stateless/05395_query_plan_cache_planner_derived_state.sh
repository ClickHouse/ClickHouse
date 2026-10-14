#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas, no-shared-merge-tree
# A plan cache hit skips analysis and planning, so any state those phases derive must either be
# represented by the cache entry or keep the query off the cache. Each case runs the identical query
# twice (a miss, then a hit) and must behave like normal execution:
#   1. The analyzer checks `max_rows_in_set` / `max_bytes_in_set` while folding an all-constant `IN`.
#      A hit carries only the folded constant, so tightening the limit must not reuse that entry.
#   2. The planner exempts a single read of `system.one` from quotas and result limits. Both the miss
#      and the hit must keep the exemption.
#   3. `ReadFromTableStep` does not serialize the `STREAM` modifier, so a streaming read must not be
#      cached: a hit would silently become an ordinary snapshot read.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

run()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $SETTINGS "$@" 2>&1 | grep -o -m1 -E 'SET_SIZE_LIMIT_EXCEEDED|TOO_MANY_ROWS_OR_BYTES|^[0-9a-z]+$'
}

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

echo "-- 1. set limits"
run --max_rows_in_set 0 --query "SELECT 2 IN (1, 2)"
run --max_rows_in_set 0 --query "SELECT 2 IN (1, 2)"
run --max_rows_in_set 1 --query "SELECT 2 IN (1, 2)"
run --max_bytes_in_set 0 --query "SELECT 3 IN (1, 3)"
run --max_bytes_in_set 0 --query "SELECT 3 IN (1, 3)"
run --max_bytes_in_set 1 --query "SELECT 3 IN (1, 3)"

echo "-- 2. result limits exemption for system.one"
run --max_result_bytes 1 --query "SELECT repeat('x', 100)"
run --max_result_bytes 1 --query "SELECT repeat('x', 100)"

echo "-- 3. streaming read"
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_stream;
    CREATE TABLE t_stream (k UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
    SYSTEM STOP MERGES t_stream;
    INSERT INTO t_stream SELECT number FROM numbers(5);
    INSERT INTO t_stream SELECT number FROM numbers(5, 5);
"
STREAM_QUERY="SELECT countIf(1) FROM t_stream STREAM BOUNDED CURSOR {'all': {'block_number': 1000000}}"
run --enable_streaming_queries 1 --query "$STREAM_QUERY"
run --enable_streaming_queries 1 --query "$STREAM_QUERY"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_stream"
