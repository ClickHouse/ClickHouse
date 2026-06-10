-- Shuffle and two-phase aggregation strategies must not be applied to
-- `GROUPING SETS` aggregations: `params.keys` is the union of all sets' keys,
-- so shuffling by the union splits the rows of one grouping-set group across
-- nodes and each node emits its own copy of the group.

SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET param__internal_cascades_cluster_node_count = 4;
-- Steer the optimizer toward the shuffle aggregation strategy.
SET param__internal_cascades_cost_config = '{"work_weight":1,"network_weight":0.01,"sequential_weight":100000}';

DROP TABLE IF EXISTS t_gsets;

CREATE TABLE t_gsets (a UInt64, b UInt64, x UInt64) ENGINE = MergeTree() ORDER BY (a, b);

-- High-NDV keys so that distributed aggregation strategies win on cost.
INSERT INTO t_gsets SELECT number % 10000, intDiv(number, 10) % 10000, number FROM numbers(1000000);

SELECT '-- 1. GROUPING SETS: aggregated totals must match the baseline';
SELECT sum(s), count() FROM (SELECT a, b, sum(x) AS s FROM t_gsets GROUP BY GROUPING SETS ((a), (b)));

SELECT '-- 2. Baseline without Cascades';
SELECT sum(s), count() FROM (SELECT a, b, sum(x) AS s FROM t_gsets GROUP BY GROUPING SETS ((a), (b)))
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

DROP TABLE t_gsets;
