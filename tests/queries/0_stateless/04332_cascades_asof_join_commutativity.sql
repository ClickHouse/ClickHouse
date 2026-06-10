-- `JoinCommutativity` must not swap ASOF joins: the "closest preceding value"
-- is resolved per left row, so the result depends on which side is which.
-- The right side is made much larger so that the (incorrectly) swapped
-- variant would win on cost via a cheaper broadcast build.

SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET param__internal_cascades_cluster_node_count = 4;

DROP TABLE IF EXISTS t_asof_small;
DROP TABLE IF EXISTS t_asof_big;

CREATE TABLE t_asof_small (k UInt64, t UInt64) ENGINE = MergeTree() ORDER BY (k, t);
CREATE TABLE t_asof_big (k UInt64, t UInt64, v UInt64) ENGINE = MergeTree() ORDER BY (k, t);

INSERT INTO t_asof_small SELECT number % 10, 100 + number FROM numbers(20);
-- `v` is a function of `(k, t)` so the result does not depend on which of the
-- duplicate `(k, t)` rows the ASOF lookup picks.
INSERT INTO t_asof_big SELECT number % 10, number % 200, (number % 10) * 1000 + number % 200 FROM numbers(50000);

SELECT '-- 1. ASOF JOIN: per-left-row closest match';
SELECT count(), sum(r.v) FROM t_asof_small AS l ASOF JOIN t_asof_big AS r ON l.k = r.k AND l.t >= r.t;

SELECT '-- 2. Baseline without Cascades';
SELECT count(), sum(r.v) FROM t_asof_small AS l ASOF JOIN t_asof_big AS r ON l.k = r.k AND l.t >= r.t
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

SELECT '-- 3. RightAny (any_join_distinct_right_table_keys) must not be swapped either';
SELECT count() FROM t_asof_small AS l ANY INNER JOIN t_asof_big AS r ON l.k = r.k
SETTINGS any_join_distinct_right_table_keys = 1;

DROP TABLE t_asof_small;
DROP TABLE t_asof_big;
