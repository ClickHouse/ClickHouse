#!/usr/bin/env bash
# Tags: no-parallel-replicas, no-random-settings, no-random-merge-tree-settings
# The test asserts the local query-plan shape, which parallel replicas would change. Random
# settings are excluded because the statistics gate decides based on the execution topology,
# which randomized read/aggregation settings legitimately change.

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

$CLIENT -q "DROP TABLE IF EXISTS t_grouped_uniq_exact"
$CLIENT -q "CREATE TABLE t_grouped_uniq_exact (k UInt32, v UInt64, n Nullable(UInt32)) ENGINE = MergeTree ORDER BY tuple()
AS SELECT number % 10, intHash64(number) % 5000, if(number % 7 = 0, NULL, toUInt32(number % 3333)) FROM numbers(1000000)"

echo "cold run: no statistics yet, a single aggregation"
$CLIENT -q "SELECT count() FROM (EXPLAIN SELECT k, uniqExact(v) FROM t_grouped_uniq_exact GROUP BY k) WHERE explain LIKE '%Aggregating%'"
$CLIENT -q "SELECT k, uniqExact(v) FROM t_grouped_uniq_exact GROUP BY k ORDER BY k"

echo "warm run: rewritten into a count over a deduplicating aggregation, same result"
arm "SELECT k, uniqExact(v) FROM t_grouped_uniq_exact GROUP BY k" > /dev/null
$CLIENT -q "SELECT replaceRegexpAll(explain, '^[^A-Za-z]+', '') FROM (EXPLAIN SELECT k, uniqExact(v) FROM t_grouped_uniq_exact GROUP BY k) WHERE explain LIKE '%Keys:%' OR explain LIKE '%Aggregates:%'"
$CLIENT -q "SELECT k, uniqExact(v) FROM t_grouped_uniq_exact GROUP BY k ORDER BY k"

echo "a NULL argument value does not count as a distinct value, exactly as in uniqExact"
$CLIENT -q "SELECT k, uniqExact(n) FROM t_grouped_uniq_exact GROUP BY k ORDER BY k"
$CLIENT -q "SELECT k, uniqExact(n) FROM t_grouped_uniq_exact GROUP BY k ORDER BY k"

echo "the disabled setting suppresses the rewrite"
$CLIENT --query_plan_rewrite_grouped_count_distinct=0 -q "SELECT count() FROM (EXPLAIN SELECT k, uniqExact(v) FROM t_grouped_uniq_exact GROUP BY k) WHERE explain LIKE '%Aggregating%'"

echo "the analyzer setting alone does not rewrite grouped queries"
$CLIENT --query_plan_rewrite_grouped_count_distinct=0 --count_distinct_optimization=1 -q "SELECT count() FROM (EXPLAIN SELECT k, uniqExact(v) FROM t_grouped_uniq_exact GROUP BY k) WHERE explain LIKE '%Aggregating%'"

echo "an argument that is itself a group key is not rewritten"
$CLIENT -q "SELECT count() FROM (EXPLAIN SELECT v, uniqExact(v) FROM t_grouped_uniq_exact GROUP BY v) WHERE explain LIKE '%Aggregating%'"

echo "WITH TOTALS is not rewritten"
$CLIENT -q "SELECT count() FROM (EXPLAIN SELECT k, uniqExact(v) FROM t_grouped_uniq_exact GROUP BY k WITH TOTALS) WHERE explain LIKE '%Aggregating%'"

echo "an observed group-key cardinality in the millions suppresses the rewrite"
$CLIENT -q "DROP TABLE IF EXISTS t_many_group_keys"
$CLIENT -q "CREATE TABLE t_many_group_keys (k UInt32, v UInt8) ENGINE = MergeTree ORDER BY tuple()
AS SELECT number, number % 3 FROM numbers(2000000)"
$CLIENT -q "SELECT k, uniqExact(v) FROM t_many_group_keys GROUP BY k FORMAT Null"
$CLIENT -q "SELECT count() FROM (EXPLAIN SELECT k, uniqExact(v) FROM t_many_group_keys GROUP BY k) WHERE explain LIKE '%Aggregating%'"
$CLIENT -q "DROP TABLE t_many_group_keys"

$CLIENT -q "DROP TABLE t_grouped_uniq_exact"
