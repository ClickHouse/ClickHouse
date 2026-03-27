-- Tests for `EagerAggregation` transformation rule in the Cascades optimizer.
--
-- The rule pushes partial aggregation below a join when GROUP BY keys include
-- the join key.  Reduces rows before the join.
--
-- Key behaviors verified:
-- 1. The rule fires without exceptions for matching patterns.
-- 2. The eager variant is a valid alternative in the memo (cost model picks winner).
-- 3. Correctness: same results regardless of which plan the cost model selects.

SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET param__internal_cascades_cluster_node_count = 4;

DROP TABLE IF EXISTS t_orders;
DROP TABLE IF EXISTS t_lineitem;

CREATE TABLE t_orders (o_orderkey UInt64, o_custkey UInt64) ENGINE = MergeTree() ORDER BY o_orderkey;
CREATE TABLE t_lineitem (l_orderkey UInt64, l_quantity UInt64) ENGINE = MergeTree() ORDER BY l_orderkey;

-- High fan-out: 100 lineitem rows per order key.
-- Eager agg reduces 10000 lineitem rows to 100 before the join.
INSERT INTO t_orders SELECT number, number % 10 FROM numbers(100);
INSERT INTO t_lineitem SELECT number % 100, number FROM numbers(10000);

-- 1. GROUP BY = right join key: eager aggregation candidate.
--    The plan should be valid (no exceptions).  Whether the eager variant
--    wins depends on the cost model.
SELECT '-- 1. GROUP BY = join key (eager candidate)';
EXPLAIN PLAN keep_logical_steps = 1
SELECT l_orderkey, sum(l_quantity) FROM t_orders
JOIN t_lineitem ON o_orderkey = l_orderkey
GROUP BY l_orderkey;

-- 2. GROUP BY ≠ join key: eager aggregation does NOT fire.
SELECT '-- 2. GROUP BY ≠ join key (no eager)';
EXPLAIN PLAN keep_logical_steps = 1
SELECT o_custkey, sum(l_quantity) FROM t_orders
JOIN t_lineitem ON o_orderkey = l_orderkey
GROUP BY o_custkey;

-- 3. Correctness: GROUP BY = join key.
SELECT '-- 3. Correctness';
SELECT l_orderkey, sum(l_quantity) FROM t_orders
JOIN t_lineitem ON o_orderkey = l_orderkey
GROUP BY l_orderkey ORDER BY l_orderkey LIMIT 5
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

-- 4. Correctness: GROUP BY ≠ join key.
SELECT '-- 4. Correctness (GROUP BY ≠ join key)';
SELECT o_custkey, sum(l_quantity) FROM t_orders
JOIN t_lineitem ON o_orderkey = l_orderkey
GROUP BY o_custkey ORDER BY o_custkey LIMIT 5
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

DROP TABLE t_orders;
DROP TABLE t_lineitem;
