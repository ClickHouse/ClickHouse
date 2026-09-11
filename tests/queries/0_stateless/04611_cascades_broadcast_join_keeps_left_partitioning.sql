-- Tags: no-darwin, no-old-analyzer
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- no-old-analyzer: distributed Cascades planning requires the analyzer.

-- A broadcast join keeps every left-side row on its node, so a left input partitioned by a key
-- still satisfies a downstream requirement for that key. Without the keyed broadcast alternative
-- the join output advertises no partitioning and the aggregation above needs an extra shuffle of
-- the joined rows.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET max_rows_to_group_by = 0;
SET param__internal_cascades_cluster_node_count = 4;
-- Make the fact side big and the dim side tiny so the join is a clear broadcast join, and keep
-- the dim table on the build side so the plan shape does not depend on a cost near-tie.
SET param__internal_join_table_stat_hints = '{"t_bk_fact": {"cardinality": 10000000, "avg_row_bytes": 16, "distinct_keys": {"k": 1000000}}, "t_bk_dim": {"cardinality": 10, "avg_row_bytes": 16, "distinct_keys": {"g": 10}}}';
SET query_plan_join_swap_table = 'false';
SET query_plan_optimize_join_order_randomize = 0;

DROP TABLE IF EXISTS t_bk_fact;
DROP TABLE IF EXISTS t_bk_dim;
-- `auto_statistics_types = ''` keeps the selectivity estimator off so the plan follows the hint
-- even when `materialize_statistics_on_insert` is randomized on.
CREATE TABLE t_bk_fact (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '';
CREATE TABLE t_bk_dim (g UInt64, name String) ENGINE = MergeTree ORDER BY g
  SETTINGS auto_statistics_types = '';
INSERT INTO t_bk_fact SELECT number % 1000, number FROM numbers(10000);
INSERT INTO t_bk_dim SELECT number, toString(number) FROM numbers(10);

-- The inner aggregation shuffles by `k` (forced shuffle aggregation) and its output stays
-- partitioned by `k` through the broadcast join, so the outer aggregation on `k` reuses it:
-- exactly one shuffle in the plan.
SELECT 'shuffles', countIf(explain LIKE '%ShuffleExchange%')
FROM (
    EXPLAIN
    SELECT k, sum(c), any(name)
    FROM (
        SELECT k, count() AS c, intDiv(k, 100) AS g
        FROM t_bk_fact
        GROUP BY k
    ) AS f
    JOIN t_bk_dim AS d ON f.g = d.g
    GROUP BY k
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1,
             distributed_plan_force_shuffle_aggregation = 1, enable_join_runtime_filters = 0
);

-- The distributed result must match the single-node plan.
SELECT 'distributed', count(), sum(s), min(k), max(k) FROM (
    SELECT k, sum(c) AS s, any(name)
    FROM (SELECT k, count() AS c, intDiv(k, 100) AS g FROM t_bk_fact GROUP BY k) AS f
    JOIN t_bk_dim AS d ON f.g = d.g
    GROUP BY k
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1,
             distributed_plan_force_shuffle_aggregation = 1, enable_join_runtime_filters = 0,
             distributed_plan_execute_locally = 1
);
SELECT 'plain', count(), sum(s), min(k), max(k) FROM (
    SELECT k, sum(c) AS s, any(name)
    FROM (SELECT k, count() AS c, intDiv(k, 100) AS g FROM t_bk_fact GROUP BY k) AS f
    JOIN t_bk_dim AS d ON f.g = d.g
    GROUP BY k
);

DROP TABLE t_bk_fact;
DROP TABLE t_bk_dim;
