-- Tags: no-random-merge-tree-settings
-- Tests for `EagerAggregation` transformation rule in the Cascades optimizer.
--
-- The rule pushes partial aggregation below a join when GROUP BY keys include
-- the join key.  Reduces rows before the join.
--
-- Key behaviors verified:
-- 1. Two-table join: GROUP BY = join key (eager fires).
-- 2. Two-table join: GROUP BY ≠ join key but from other side (eager fires).
-- 3. Multi-join: eager pushes through intermediate joins (chain reconstruction).
-- 4. Non-candidate: GROUP BY key not in any join side.
-- 5-6. Correctness checks.
-- 7-8. Non-equi predicate: eager must not fire (single join).
-- 9. Non-equi predicate: multi-join correctness.

SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET param__internal_cascades_cluster_node_count = 4;

DROP TABLE IF EXISTS t_orders;
DROP TABLE IF EXISTS t_lineitem;
DROP TABLE IF EXISTS t_customer;

CREATE TABLE t_customer (c_custkey UInt64, c_name String) ENGINE = MergeTree() ORDER BY c_custkey;
CREATE TABLE t_orders (o_orderkey UInt64, o_custkey UInt64) ENGINE = MergeTree() ORDER BY o_orderkey;
CREATE TABLE t_lineitem (l_orderkey UInt64, l_quantity UInt64) ENGINE = MergeTree() ORDER BY l_orderkey;

-- High fan-out: 100 lineitem rows per order key.
INSERT INTO t_customer SELECT number, concat('Customer#', toString(number)) FROM numbers(10);
INSERT INTO t_orders SELECT number, number % 10 FROM numbers(100);
INSERT INTO t_lineitem SELECT number % 100, number FROM numbers(10000);

-- 1. GROUP BY = right join key: eager aggregation candidate.
SELECT '-- 1. GROUP BY = join key (eager candidate)';
EXPLAIN PLAN keep_logical_steps = 1
SELECT l_orderkey, sum(l_quantity) FROM t_orders
JOIN t_lineitem ON o_orderkey = l_orderkey
GROUP BY l_orderkey;

-- 2. GROUP BY from other side: eager still fires (key is on the other side).
SELECT '-- 2. GROUP BY from other side (eager fires)';
EXPLAIN PLAN keep_logical_steps = 1
SELECT o_custkey, sum(l_quantity) FROM t_orders
JOIN t_lineitem ON o_orderkey = l_orderkey
GROUP BY o_custkey;

-- 3. Multi-join: push partial agg through intermediate join.
--    customer ⋈ (orders ⋈ lineitem), GROUP BY c_custkey.
--    The rule should push partial agg of lineitem below the inner join,
--    then reconstruct the outer join with customer.
SELECT '-- 3. Multi-join chain reconstruction';
EXPLAIN PLAN keep_logical_steps = 1
SELECT c_custkey, c_name, sum(l_quantity) FROM t_customer
JOIN t_orders ON c_custkey = o_custkey
JOIN t_lineitem ON o_orderkey = l_orderkey
GROUP BY c_custkey, c_name;

-- 4. Non-candidate: GROUP BY key not available from any join side.
SELECT '-- 4. Non-candidate';
EXPLAIN PLAN keep_logical_steps = 1
SELECT l_orderkey % 5 AS bucket, sum(l_quantity) FROM t_orders
JOIN t_lineitem ON o_orderkey = l_orderkey
GROUP BY bucket;

-- 5. Correctness: GROUP BY = join key.
SELECT '-- 5. Correctness (GROUP BY = join key)';
SELECT l_orderkey, sum(l_quantity) FROM t_orders
JOIN t_lineitem ON o_orderkey = l_orderkey
GROUP BY l_orderkey ORDER BY l_orderkey LIMIT 5
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

-- 6. Correctness: multi-join.
SELECT '-- 6. Correctness (multi-join)';
SELECT c_custkey, c_name, sum(l_quantity) FROM t_customer
JOIN t_orders ON c_custkey = o_custkey
JOIN t_lineitem ON o_orderkey = l_orderkey
GROUP BY c_custkey, c_name ORDER BY c_custkey LIMIT 5
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

-- 7. Non-equi predicate: eager aggregation must NOT fire.
--    reconstructJoin only rebuilds equality predicates, so non-equi conditions
--    would be silently dropped, producing wrong results.
DROP TABLE IF EXISTS t_events;
DROP TABLE IF EXISTS t_sessions;
CREATE TABLE t_events (id UInt64, ts UInt64, value UInt64) ENGINE = MergeTree() ORDER BY id
    SETTINGS index_granularity = 8192, auto_statistics_types = '';
CREATE TABLE t_sessions (id UInt64, start_ts UInt64) ENGINE = MergeTree() ORDER BY id
    SETTINGS index_granularity = 8192, auto_statistics_types = '';
-- Enough data so the optimizer considers eager aggregation worthwhile.
-- Without the fix, EagerAggregation fires and reconstructJoin drops the
-- non-equi predicate, producing a malformed plan that crashes the server.
INSERT INTO t_events SELECT number % 50, number, number FROM numbers(5000);
INSERT INTO t_sessions SELECT number, number * 100 FROM numbers(50);

SELECT '-- 7. Non-equi predicate (eager must not fire)';
EXPLAIN PLAN keep_logical_steps = 1
SELECT t_sessions.id, sum(value)
FROM t_sessions JOIN t_events ON t_sessions.id = t_events.id AND t_events.ts > t_sessions.start_ts
GROUP BY t_sessions.id;

SELECT '-- 8. Correctness: non-equi predicate';
SELECT t_sessions.id, sum(value) as s
FROM t_sessions JOIN t_events ON t_sessions.id = t_events.id AND t_events.ts > t_sessions.start_ts
GROUP BY t_sessions.id ORDER BY t_sessions.id LIMIT 5;

SELECT t_sessions.id, sum(value) as s
FROM t_sessions JOIN t_events ON t_sessions.id = t_events.id AND t_events.ts > t_sessions.start_ts
GROUP BY t_sessions.id ORDER BY t_sessions.id LIMIT 5
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

DROP TABLE t_events;
DROP TABLE t_sessions;

-- 9. Multi-join with non-equi predicate on one join.
--    The non-equi join is a barrier: eager aggregation must not push through it.
--    Correctness check: compare cascades vs baseline.
SELECT '-- 9. Multi-join with non-equi predicate';
SELECT c_custkey, c_name, sum(l_quantity) as s FROM t_customer
JOIN t_orders ON c_custkey = o_custkey AND o_orderkey > c_custkey
JOIN t_lineitem ON o_orderkey = l_orderkey
GROUP BY c_custkey, c_name ORDER BY c_custkey LIMIT 5;

SELECT c_custkey, c_name, sum(l_quantity) as s FROM t_customer
JOIN t_orders ON c_custkey = o_custkey AND o_orderkey > c_custkey
JOIN t_lineitem ON o_orderkey = l_orderkey
GROUP BY c_custkey, c_name ORDER BY c_custkey LIMIT 5
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

DROP TABLE t_customer;
DROP TABLE t_orders;
DROP TABLE t_lineitem;
