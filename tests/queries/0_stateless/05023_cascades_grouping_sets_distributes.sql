-- Tags: no-darwin, no-old-analyzer
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- no-old-analyzer: distributed Cascades planning requires the analyzer, like the other make_distributed_plan tests.

-- A `GROUPING SETS` aggregation distributes with the two-phase split: every worker builds
-- partial states for every grouping set over its share of the data, tagged with
-- `__grouping_set`, and one node merges them per set. The shuffle strategy stays inapplicable:
-- rows of one subtotal group would land in several buckets and the group would be emitted
-- several times.

SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET max_rows_to_group_by = 0;
SET param__internal_cascades_cluster_node_count = 4;

DROP TABLE IF EXISTS t_gs_cascades;

CREATE TABLE t_gs_cascades (k1 String, k2 UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';

INSERT INTO t_gs_cascades SELECT 'k' || (number % 3)::String, number % 2, number FROM numbers(1000);

-- A large table-size hint makes Cascades pick the distributed two-phase plan (partial
-- aggregation per worker, gathered and merged) rather than reading and aggregating everything
-- on one node.
SET param__internal_join_table_stat_hints = '{"t_gs_cascades": {"cardinality": 600000000, "avg_row_bytes": 24, "distinct_keys": {"k1": 3, "k2": 2}}}';

SELECT '-- the plan splits the grouping sets aggregation into partial and merge';
-- `distributed_aggregation_memory_efficient` is pinned on: the merge must keep it off for
-- grouping sets, so no `Mode` line may appear in the dump.
EXPLAIN distributed = 1 SELECT k1, k2, grouping(k1) + grouping(k2) AS g, sum(v)
FROM t_gs_cascades GROUP BY GROUPING SETS ((k1), (k2)) ORDER BY ALL
SETTINGS distributed_aggregation_memory_efficient = 1;

SELECT '-- results';
SELECT k1, k2, grouping(k1) + grouping(k2) AS g, sum(v), count()
FROM t_gs_cascades GROUP BY GROUPING SETS ((k1), (k2), ()) ORDER BY ALL;

SELECT '-- same result from the non-distributed baseline';
SELECT k1, k2, grouping(k1) + grouping(k2) AS g, sum(v), count()
FROM t_gs_cascades GROUP BY GROUPING SETS ((k1), (k2), ()) ORDER BY ALL
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

-- Force the two-level aggregation and a parallel partial flush; with the memory-efficient merge
-- wrongly enabled for grouping sets some groups would be emitted twice. The check is repeated
-- because such duplication depends on chunk arrival order and would not appear on every run.
SET distributed_aggregation_memory_efficient = 1;
SET group_by_two_level_threshold = 1;
SET max_threads = 4;

SELECT '-- memory-efficient setting on: no duplicate groups';
SELECT throwIf(count() != uniqExact((k1, k2, g)), 'duplicate grouping set groups') FROM (SELECT k1, k2, grouping(k1) + grouping(k2) AS g, sum(v) FROM t_gs_cascades GROUP BY GROUPING SETS ((k1), (k2)));
SELECT throwIf(count() != uniqExact((k1, k2, g)), 'duplicate grouping set groups') FROM (SELECT k1, k2, grouping(k1) + grouping(k2) AS g, sum(v) FROM t_gs_cascades GROUP BY GROUPING SETS ((k1), (k2)));
SELECT throwIf(count() != uniqExact((k1, k2, g)), 'duplicate grouping set groups') FROM (SELECT k1, k2, grouping(k1) + grouping(k2) AS g, sum(v) FROM t_gs_cascades GROUP BY GROUPING SETS ((k1), (k2)));

SELECT '-- the split still happens under distributed_plan_force_shuffle_aggregation';
-- The shuffle strategy does not exist for grouping sets, so the force-shuffle setting cannot
-- push the aggregation back to a single node.
EXPLAIN distributed = 1 SELECT k1, k2, grouping(k1) + grouping(k2) AS g, sum(v)
FROM t_gs_cascades GROUP BY GROUPING SETS ((k1), (k2)) ORDER BY ALL
SETTINGS distributed_plan_force_shuffle_aggregation = 1;

DROP TABLE t_gs_cascades;
