#!/usr/bin/env bash
# Tags: no-parallel-replicas, no-random-settings, no-random-merge-tree-settings
# The test asserts the local query-plan shape, which parallel replicas would change. Random
# settings are excluded because the statistics gate decides based on the execution topology,
# which randomized read/aggregation settings legitimately change.

# Gate behavior of the grouped count-distinct rewrite across executions: statistics recorded by
# one aggregation must not drive another aggregation's rewrite, a decision warmed by a wide
# execution must not apply to a narrow one, and a favorable decision must not stay latched after
# the data drifts to an unfavorable shape.

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

$CLIENT -q "DROP TABLE IF EXISTS t_cd_gate"
$CLIENT -q "CREATE TABLE t_cd_gate (k UInt32, s String, lc LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple()
AS SELECT number % 10, concat('s', toString(intHash64(number) % 3000)), concat('l', toString(intHash32(number) % 2000))
FROM numbers(1000000)"

echo "statistics of uniqExact(s) do not rewrite uniqExact(lc) on its first run"
$CLIENT -q "SELECT k, uniqExact(s) FROM t_cd_gate GROUP BY k ORDER BY k LIMIT 2"
arm "SELECT k, uniqExact(s) FROM t_cd_gate GROUP BY k"
$CLIENT -q "SELECT count() FROM (EXPLAIN SELECT k, uniqExact(lc) FROM t_cd_gate GROUP BY k) WHERE explain LIKE '%Aggregating%'"

echo "a decision warmed by a wide execution does not apply to a single-threaded one"
$CLIENT --max_threads=1 -q "SELECT count() FROM (EXPLAIN SELECT k, uniqExact(s) FROM t_cd_gate GROUP BY k) WHERE explain LIKE '%Aggregating%'"

echo "the rewritten runs refresh the gate: unfavorable data drift turns the rewrite back off"
# The inserted rows make the group keys millions and the per-key distinct sets singletons — the
# shape where the rewrite loses. The first query after the insert still runs rewritten (the gate
# decided from the stale entry), but its created aggregations record the drifted group-key and
# pair counts back onto the original aggregation's entry, and the next plan steps aside. The
# drift run must use the aggregate's output: wrapping it in `count()` would let the unused-column
# removal drop the `uniqExact` entirely.
$CLIENT -q "INSERT INTO t_cd_gate SELECT 10 + number, 's_unique', 'l' FROM numbers(2000000)"
$CLIENT -q "SELECT k, uniqExact(s) FROM t_cd_gate GROUP BY k ORDER BY k LIMIT 1"
$CLIENT -q "SELECT count() FROM (EXPLAIN SELECT k, uniqExact(s) FROM t_cd_gate GROUP BY k) WHERE explain LIKE '%Aggregating%'"

$CLIENT -q "DROP TABLE t_cd_gate"
