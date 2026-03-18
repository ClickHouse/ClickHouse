-- Correctness test: broadcast join must not be used when the replicated side
-- produces output rows in a semi/anti join.  Broadcasting the output side causes
-- duplicate rows across nodes — each node independently matches its local probe
-- slice against the full replicated build side, emitting the same output row from
-- multiple nodes.  Two-phase aggregation then sums the duplicates.

SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET param__internal_cascades_cluster_node_count = 4;
-- Low sequential_weight to aggressively prefer broadcast — without the fix,
-- the optimizer would choose Broadcast for the semi-join.
SET param__internal_cascades_cost_config = '{"sequential_weight":1}';

DROP TABLE IF EXISTS test_orders;
DROP TABLE IF EXISTS test_lineitem;

CREATE TABLE test_orders (
    o_orderkey UInt64,
    o_priority String
) ENGINE = MergeTree ORDER BY o_orderkey;

CREATE TABLE test_lineitem (
    l_orderkey UInt64,
    l_linenumber UInt32
) ENGINE = MergeTree ORDER BY l_orderkey;

SYSTEM STOP MERGES test_orders;
INSERT INTO test_orders SELECT number, 'P' || toString(number % 5) FROM numbers(250);
INSERT INTO test_orders SELECT number, 'P' || toString(number % 5) FROM numbers(250, 250);
INSERT INTO test_orders SELECT number, 'P' || toString(number % 5) FROM numbers(500, 250);
INSERT INTO test_orders SELECT number, 'P' || toString(number % 5) FROM numbers(750, 250);

SYSTEM STOP MERGES test_lineitem;
INSERT INTO test_lineitem SELECT number % 1000, number FROM numbers(500);
INSERT INTO test_lineitem SELECT number % 1000, number FROM numbers(500, 500);
INSERT INTO test_lineitem SELECT number % 1000, number FROM numbers(1000, 500);
INSERT INTO test_lineitem SELECT number % 1000, number FROM numbers(1500, 500);

-- 1. Verify plan uses Shuffle, not Broadcast, for the semi-join.
EXPLAIN PLAN keep_logical_steps = 1
SELECT count() FROM test_orders
WHERE EXISTS (SELECT 1 FROM test_lineitem WHERE l_orderkey = o_orderkey);

-- 2. Execute with distributed_plan_execute_locally to simulate multi-node execution.
--    With the fix (Shuffle): correct result = 1000.
--    Without the fix (Broadcast): each order counted once per node that has matching
--    lineitem rows → inflated result (>1000 on a real multi-node cluster).
SELECT count() FROM test_orders
WHERE EXISTS (SELECT 1 FROM test_lineitem WHERE l_orderkey = o_orderkey)
SETTINGS distributed_plan_execute_locally = 1;

SELECT count() FROM test_orders
WHERE EXISTS (SELECT 1 FROM test_lineitem WHERE l_orderkey = o_orderkey)
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

-- Test with explicit RIGHT SEMI JOIN
EXPLAIN PLAN keep_logical_steps = 1
SELECT count() FROM test_lineitem RIGHT SEMI JOIN test_orders ON l_orderkey = o_orderkey;

SELECT count() FROM test_lineitem RIGHT SEMI JOIN test_orders ON l_orderkey = o_orderkey
SETTINGS distributed_plan_execute_locally = 1;

SELECT count() FROM test_lineitem RIGHT SEMI JOIN test_orders ON l_orderkey = o_orderkey
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

-- Test with explicit LEFT SEMI JOIN
EXPLAIN PLAN keep_logical_steps = 1
SELECT count() FROM test_orders LEFT SEMI JOIN test_lineitem ON l_orderkey = o_orderkey;

SELECT count() FROM test_orders LEFT SEMI JOIN test_lineitem ON l_orderkey = o_orderkey
SETTINGS distributed_plan_execute_locally = 1;

SELECT count() FROM test_orders LEFT SEMI JOIN test_lineitem ON l_orderkey = o_orderkey
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

DROP TABLE test_orders;
DROP TABLE test_lineitem;
