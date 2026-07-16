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

$CLIENT -q "DROP TABLE IF EXISTS t_cd_shapes"
$CLIENT -q "CREATE TABLE t_cd_shapes (k UInt32, k2 UInt32, s String, lc LowCardinality(String), v UInt64) ENGINE = MergeTree ORDER BY tuple()
AS SELECT number % 5, number % 3, concat('s', toString(number % 2503)), concat('lc', toString(number % 1499)),
          if(number % 5 = 0, 0, intHash64(number) % 2003)
FROM numbers(1000000)"

echo "String argument"
$CLIENT -q "SELECT k, uniqExact(s) AS u FROM t_cd_shapes GROUP BY k ORDER BY k"
arm "SELECT k, uniqExact(s) AS u FROM t_cd_shapes GROUP BY k"
$CLIENT -q "SELECT k, uniqExact(s) AS u FROM t_cd_shapes GROUP BY k ORDER BY k"

echo "LowCardinality argument"
$CLIENT -q "SELECT k, uniqExact(lc) AS u FROM t_cd_shapes GROUP BY k ORDER BY k"
arm "SELECT k, uniqExact(lc) AS u FROM t_cd_shapes GROUP BY k"
$CLIENT -q "SELECT k, uniqExact(lc) AS u FROM t_cd_shapes GROUP BY k ORDER BY k"

echo "expression argument"
$CLIENT -q "SELECT k, uniqExact(v * 2 + 1) AS u FROM t_cd_shapes GROUP BY k ORDER BY k"
arm "SELECT k, uniqExact(v * 2 + 1) AS u FROM t_cd_shapes GROUP BY k"
$CLIENT -q "SELECT k, uniqExact(v * 2 + 1) AS u FROM t_cd_shapes GROUP BY k ORDER BY k"

echo "multiple group keys"
$CLIENT -q "SELECT k, k2, uniqExact(v) AS u FROM t_cd_shapes GROUP BY k, k2 ORDER BY k, k2 LIMIT 6"
arm "SELECT k, k2, uniqExact(v) AS u FROM t_cd_shapes GROUP BY k, k2"
$CLIENT -q "SELECT k, k2, uniqExact(v) AS u FROM t_cd_shapes GROUP BY k, k2 ORDER BY k, k2 LIMIT 6"

echo "HAVING above the aggregation"
$CLIENT -q "SELECT k, uniqExact(v) AS u FROM t_cd_shapes GROUP BY k HAVING u > 100 ORDER BY k"
arm "SELECT k, uniqExact(v) AS u FROM t_cd_shapes GROUP BY k HAVING u > 100"
$CLIENT -q "SELECT k, uniqExact(v) AS u FROM t_cd_shapes GROUP BY k HAVING u > 100 ORDER BY k"

echo "WHERE below the aggregation"
$CLIENT -q "SELECT k, uniqExact(v) AS u FROM t_cd_shapes WHERE k >= 3 GROUP BY k ORDER BY k"
arm "SELECT k, uniqExact(v) AS u FROM t_cd_shapes WHERE k >= 3 GROUP BY k"
$CLIENT -q "SELECT k, uniqExact(v) AS u FROM t_cd_shapes WHERE k >= 3 GROUP BY k ORDER BY k"

echo "count(DISTINCT ...) resolves to uniqExact and is rewritten"
$CLIENT -q "SELECT k, count(DISTINCT v) AS u FROM t_cd_shapes GROUP BY k ORDER BY k"
arm "SELECT k, count(DISTINCT v) AS u FROM t_cd_shapes GROUP BY k"
$CLIENT -q "SELECT k, count(DISTINCT v) AS u FROM t_cd_shapes GROUP BY k ORDER BY k"

echo "a different function (uniq) is not rewritten"
$CLIENT -q "SELECT k, uniq(v) AS u FROM t_cd_shapes GROUP BY k ORDER BY k"
$CLIENT -q "SELECT count() FROM (EXPLAIN SELECT k, uniq(v) AS u FROM t_cd_shapes GROUP BY k) WHERE explain LIKE '%Aggregating%'"

echo "a combinator (uniqExactIf) is not rewritten"
$CLIENT -q "SELECT k, uniqExactIf(v, k >= 3) AS u FROM t_cd_shapes GROUP BY k ORDER BY k"
$CLIENT -q "SELECT count() FROM (EXPLAIN SELECT k, uniqExactIf(v, k >= 3) AS u FROM t_cd_shapes GROUP BY k) WHERE explain LIKE '%Aggregating%'"

echo "multiple arguments are not rewritten"
$CLIENT -q "SELECT k, uniqExact(s, v) AS u FROM t_cd_shapes GROUP BY k ORDER BY k"
$CLIENT -q "SELECT count() FROM (EXPLAIN SELECT k, uniqExact(s, v) AS u FROM t_cd_shapes GROUP BY k) WHERE explain LIKE '%Aggregating%'"

echo "ROLLUP is not rewritten"
$CLIENT -q "SELECT k % 3 AS a, uniqExact(v) AS u FROM t_cd_shapes GROUP BY a WITH ROLLUP ORDER BY a, u"
$CLIENT -q "SELECT count() FROM (EXPLAIN SELECT k % 3 AS a, uniqExact(v) AS u FROM t_cd_shapes GROUP BY a WITH ROLLUP) WHERE explain LIKE '%Aggregating%'"

$CLIENT -q "DROP TABLE t_cd_shapes"
