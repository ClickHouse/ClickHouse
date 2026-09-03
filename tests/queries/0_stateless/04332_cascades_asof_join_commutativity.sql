-- Tags: no-darwin, no-old-analyzer
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- no-old-analyzer: distributed Cascades planning requires the analyzer, like the other make_distributed_plan tests.

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
SET query_plan_optimize_join_order_randomize = 0;

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

-- A plain INNER ALL join is commutable even when `USING` casts a mismatched key type to the
-- supertype: `swapInputs` remaps the cast to the new side. The swap wins here (20-row build side
-- instead of 50000), so the plan must contain a `swapped` join and the result must stay correct.
SELECT '-- 4. USING join with mismatched key types is swapped and stays correct';
DROP TABLE IF EXISTS t_tc_small;
CREATE TABLE t_tc_small (k UInt32) ENGINE = MergeTree() ORDER BY k;
INSERT INTO t_tc_small SELECT number % 10 FROM numbers(20);
-- The outer query runs without Cascades (it reads from the `viewExplain` table function, which the
-- optimizer cannot clone); the inner EXPLAIN re-enables it explicitly.
SELECT sum(explain LIKE '%swapped%') FROM (
    EXPLAIN PLAN keep_logical_steps = 1
    SELECT count() FROM t_tc_small INNER JOIN t_asof_big USING (k)
    SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1
) SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;
SELECT count() FROM t_tc_small INNER JOIN t_asof_big USING (k);
SELECT count() FROM t_tc_small INNER JOIN t_asof_big USING (k)
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

DROP TABLE t_tc_small;
DROP TABLE t_asof_small;
DROP TABLE t_asof_big;
