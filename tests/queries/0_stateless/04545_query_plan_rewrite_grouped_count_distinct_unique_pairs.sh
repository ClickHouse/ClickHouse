#!/usr/bin/env bash
# Tags: no-parallel-replicas, no-random-settings, no-random-merge-tree-settings
# The test asserts the local query-plan shape, which parallel replicas would change. Random
# settings are excluded because the statistics gate decides based on the execution topology,
# which randomized read/aggregation settings legitimately change.

# Both sides of the gate's pair-duplication condition. When every (group key, argument value)
# pair is unique, the deduplicating aggregation removes nothing and the rewrite only adds work —
# the gate must keep it off. When pairs are heavily duplicated, the rewrite pays off and the
# decision must survive rewritten executions: the created aggregations record the group-key,
# pair, and source-row counts back onto the original aggregation's entry, and those must not be
# mistaken for an unfavorable data shape.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The rewrite does not apply under a group-by row limit, which a server profile may set. The
# thread count is pinned because the statistics gate requires group keys shared across several
# reading streams; `merge_tree_min_rows_for_concurrent_read = 1` maximizes the number of read
# tasks so that all pinned threads normally participate.
CLIENT="$CLICKHOUSE_CLIENT --query_plan_rewrite_grouped_count_distinct=1 --max_threads=4 --max_rows_to_group_by=0 --merge_tree_min_rows_for_concurrent_read=1"

# Runs the query until its recorded statistics arm the rewrite gate and prints the aggregating
# step count of the final plan. Rows may be distributed across the reading threads in any way,
# including all of them going to one thread — a warm run made under such scheduling records no
# cross-thread key sharing and legitimately does not arm the gate, so arming is retried.
function arm()
{
    local query=$1
    local steps=""
    for _ in {1..30}; do
        steps=$($CLIENT -q "SELECT count() FROM (EXPLAIN $query) WHERE explain LIKE '%Aggregating%'")
        if [ "$steps" == "2" ]; then break; fi
        $CLIENT -q "$query FORMAT Null"
    done
    echo "$steps"
}

$CLIENT -q "DROP TABLE IF EXISTS t_cd_unique_pairs"
$CLIENT -q "DROP TABLE IF EXISTS t_cd_dup_pairs"

echo "unique pairs never fire"
$CLIENT -q "CREATE TABLE t_cd_unique_pairs (k UInt32, x UInt64) ENGINE = MergeTree ORDER BY tuple()
AS SELECT number % 10, number FROM numbers(1000000)"
$CLIENT -q "SELECT k, uniqExact(x) FROM t_cd_unique_pairs GROUP BY k ORDER BY k LIMIT 1"
$CLIENT -q "SELECT count() FROM (EXPLAIN SELECT k, uniqExact(x) FROM t_cd_unique_pairs GROUP BY k) WHERE explain LIKE '%Aggregating%'"

echo "duplicated pairs stay on across rewritten executions"
# Every (k, x) pair occurs 20 times, spread 50000 rows apart so all reading streams see it.
$CLIENT -q "CREATE TABLE t_cd_dup_pairs (k UInt32, x UInt64) ENGINE = MergeTree ORDER BY tuple()
AS SELECT number % 10, intHash64(number % 50000) FROM numbers(1000000)"
$CLIENT -q "SELECT k, uniqExact(x) FROM t_cd_dup_pairs GROUP BY k ORDER BY k LIMIT 1"
arm "SELECT k, uniqExact(x) FROM t_cd_dup_pairs GROUP BY k"
$CLIENT -q "SELECT k, uniqExact(x) FROM t_cd_dup_pairs GROUP BY k ORDER BY k LIMIT 1"
$CLIENT -q "SELECT count() FROM (EXPLAIN SELECT k, uniqExact(x) FROM t_cd_dup_pairs GROUP BY k) WHERE explain LIKE '%Aggregating%'"
$CLIENT -q "SELECT k, uniqExact(x) FROM t_cd_dup_pairs GROUP BY k ORDER BY k LIMIT 1"
$CLIENT -q "SELECT count() FROM (EXPLAIN SELECT k, uniqExact(x) FROM t_cd_dup_pairs GROUP BY k) WHERE explain LIKE '%Aggregating%'"

$CLIENT -q "DROP TABLE t_cd_unique_pairs"
$CLIENT -q "DROP TABLE t_cd_dup_pairs"
