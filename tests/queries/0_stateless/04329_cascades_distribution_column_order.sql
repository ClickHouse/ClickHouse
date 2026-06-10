-- Distribution-column matching in `isDistributionSatisfiedBy` must be
-- positional: the partition hash is computed over the key columns in order,
-- so data shuffled by `(b, a)` does not colocate with data shuffled by
-- `(a, b)`. A subquery aggregated with `GROUP BY b, a` must be re-shuffled
-- when the parent join requires distribution by `(a, b)`.

SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET param__internal_cascades_cluster_node_count = 4;
-- Steer the optimizer toward shuffle aggregation for the subquery so that the
-- (b, a)-distributed aggregation output is considered for reuse by the join.
SET param__internal_cascades_cost_config = '{"work_weight":1,"network_weight":0.01,"sequential_weight":100000}';

DROP TABLE IF EXISTS t_dist_left;
DROP TABLE IF EXISTS t_dist_right;

CREATE TABLE t_dist_left (a UInt64, b UInt64, x UInt64) ENGINE = MergeTree() ORDER BY (a, b);
CREATE TABLE t_dist_right (a UInt64, b UInt64, y UInt64) ENGINE = MergeTree() ORDER BY (a, b);

-- High-NDV group keys so that shuffle aggregation wins for the subquery.
INSERT INTO t_dist_left SELECT number % 1000, intDiv(number, 1000) % 100, number FROM numbers(100000);
INSERT INTO t_dist_right SELECT number % 1000, intDiv(number, 1000) % 100, number * 3 FROM numbers(1000000);

SELECT '-- 1. Join on (a, b) with subquery grouped by (b, a): all rows must match';
SELECT count(), sum(l.x + sq.s)
FROM t_dist_left AS l
JOIN (SELECT b, a, sum(y) AS s FROM t_dist_right GROUP BY b, a) AS sq
ON l.a = sq.a AND l.b = sq.b;

SELECT '-- 2. Baseline without Cascades';
SELECT count(), sum(l.x + sq.s)
FROM t_dist_left AS l
JOIN (SELECT b, a, sum(y) AS s FROM t_dist_right GROUP BY b, a) AS sq
ON l.a = sq.a AND l.b = sq.b
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

DROP TABLE t_dist_left;
DROP TABLE t_dist_right;
