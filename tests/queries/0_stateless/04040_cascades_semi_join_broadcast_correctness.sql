-- Correctness test: broadcast join must not be used when the replicated side
-- produces output rows in a semi/anti join.  Broadcasting the output side causes
-- duplicate rows across nodes — each node independently matches its local probe
-- slice against the full replicated build side, emitting the same output row from
-- multiple nodes.  Two-phase aggregation then sums the duplicates.
--
-- To exercise the broadcast path, the filter table (test_lineitem) is made very
-- small so the optimizer prefers to broadcast it rather than shuffle both sides.

SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET param__internal_cascades_cluster_node_count = 4;
-- Low sequential_weight so broadcast's higher per-node sequential cost doesn't
-- dominate.  With a tiny filter table, broadcast is clearly cheaper on network.
SET param__internal_cascades_cost_config = '{"sequential_weight":1}';

DROP TABLE IF EXISTS test_orders;
DROP TABLE IF EXISTS test_lineitem;

CREATE TABLE test_orders (
    o_orderkey UInt64,
    o_priority String
) ENGINE = MergeTree ORDER BY o_orderkey;

-- Small filter table: only 20 rows, so broadcast is clearly preferred.
CREATE TABLE test_lineitem (
    l_orderkey UInt64,
    l_linenumber UInt32
) ENGINE = MergeTree ORDER BY l_orderkey;

SYSTEM STOP MERGES test_orders;
INSERT INTO test_orders SELECT number, 'P' || toString(number % 5) FROM numbers(250);
INSERT INTO test_orders SELECT number, 'P' || toString(number % 5) FROM numbers(250, 250);
INSERT INTO test_orders SELECT number, 'P' || toString(number % 5) FROM numbers(500, 250);
INSERT INTO test_orders SELECT number, 'P' || toString(number % 5) FROM numbers(750, 250);

-- Only 20 unique keys matching orders 0..19, so semi-join output = 20 rows.
SYSTEM STOP MERGES test_lineitem;
INSERT INTO test_lineitem SELECT number, 1 FROM numbers(20);

-- 1. Verify plan uses Broadcast with the correct side (lineitem is broadcast,
--    orders is the probe/output side via ParallelRead).
EXPLAIN PLAN keep_logical_steps = 1
SELECT count() FROM test_orders
WHERE EXISTS (SELECT 1 FROM test_lineitem WHERE l_orderkey = o_orderkey);

-- 2. Execute with distributed_plan_execute_locally to simulate multi-node execution.
--    Correct result = 20 (only 20 orders have matching lineitem keys).
--    If the wrong side were broadcast, each matching order would be counted once
--    per node → inflated result.
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
