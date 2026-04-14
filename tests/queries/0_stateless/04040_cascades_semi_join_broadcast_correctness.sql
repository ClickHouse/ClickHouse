-- Correctness test: broadcast join must not be used when the replicated (right)
-- side can produce output rows.  Broadcasting it causes duplicate rows across
-- nodes — each node independently marks right-side rows as "unmatched" based
-- on its local left slice.
--
-- This affects RIGHT joins (all strictness) and FULL joins.
-- The filter table (test_lineitem) is small so the optimizer prefers broadcast
-- when it is safe — these tests verify that broadcast is blocked when unsafe.

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
) ENGINE = MergeTree ORDER BY o_orderkey
  SETTINGS index_granularity = 8192, auto_statistics_types = '';

-- Small filter table: only 20 rows, so broadcast is clearly preferred.
CREATE TABLE test_lineitem (
    l_orderkey UInt64,
    l_linenumber UInt32
) ENGINE = MergeTree ORDER BY l_orderkey
  SETTINGS index_granularity = 8192, auto_statistics_types = '';

SYSTEM STOP MERGES test_orders;
INSERT INTO test_orders SELECT number, 'P' || toString(number % 5) FROM numbers(250);
INSERT INTO test_orders SELECT number, 'P' || toString(number % 5) FROM numbers(250, 250);
INSERT INTO test_orders SELECT number, 'P' || toString(number % 5) FROM numbers(500, 250);
INSERT INTO test_orders SELECT number, 'P' || toString(number % 5) FROM numbers(750, 250);

-- Only 20 unique keys matching orders 0..19, so semi-join output = 20 rows.
SYSTEM STOP MERGES test_lineitem;
INSERT INTO test_lineitem SELECT number, 1 FROM numbers(20);

SELECT '-- EXISTS (rewritten to LEFT SEMI)';
EXPLAIN PLAN keep_logical_steps = 1
SELECT count() FROM test_orders
WHERE EXISTS (SELECT 1 FROM test_lineitem WHERE l_orderkey = o_orderkey);

SELECT count() FROM test_orders
WHERE EXISTS (SELECT 1 FROM test_lineitem WHERE l_orderkey = o_orderkey)
SETTINGS distributed_plan_execute_locally = 1;

SELECT count() FROM test_orders
WHERE EXISTS (SELECT 1 FROM test_lineitem WHERE l_orderkey = o_orderkey)
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT '-- RIGHT SEMI JOIN (commuted to LEFT SEMI: orders=ParallelRead, lineitem=Broadcast)';
EXPLAIN PLAN keep_logical_steps = 1
SELECT count() FROM test_lineitem RIGHT SEMI JOIN test_orders ON l_orderkey = o_orderkey;

SELECT count() FROM test_lineitem RIGHT SEMI JOIN test_orders ON l_orderkey = o_orderkey
SETTINGS distributed_plan_execute_locally = 1;

SELECT count() FROM test_lineitem RIGHT SEMI JOIN test_orders ON l_orderkey = o_orderkey
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

-- RightAny is not commutable, so broadcast guard must block it.
SELECT '-- RIGHT ANY JOIN (RightAny strictness, not commutable)';
EXPLAIN PLAN keep_logical_steps = 1
SELECT count() FROM test_orders RIGHT ANY JOIN test_lineitem ON o_orderkey = l_orderkey
SETTINGS any_join_distinct_right_table_keys = 1;

SELECT count() FROM test_orders RIGHT ANY JOIN test_lineitem ON o_orderkey = l_orderkey
SETTINGS distributed_plan_execute_locally = 1, any_join_distinct_right_table_keys = 1;

SELECT count() FROM test_orders RIGHT ANY JOIN test_lineitem ON o_orderkey = l_orderkey
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0, any_join_distinct_right_table_keys = 1;

SELECT '-- LEFT SEMI JOIN (lineitem broadcast, no swap needed)';
EXPLAIN PLAN keep_logical_steps = 1
SELECT count() FROM test_orders LEFT SEMI JOIN test_lineitem ON l_orderkey = o_orderkey;

SELECT count() FROM test_orders LEFT SEMI JOIN test_lineitem ON l_orderkey = o_orderkey
SETTINGS distributed_plan_execute_locally = 1;

SELECT count() FROM test_orders LEFT SEMI JOIN test_lineitem ON l_orderkey = o_orderkey
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT '-- RIGHT ANTI JOIN (commuted to LEFT ANTI: orders=ParallelRead, lineitem=Broadcast)';
EXPLAIN PLAN keep_logical_steps = 1
SELECT count() FROM test_lineitem RIGHT ANTI JOIN test_orders ON l_orderkey = o_orderkey;

SELECT count() FROM test_lineitem RIGHT ANTI JOIN test_orders ON l_orderkey = o_orderkey
SETTINGS distributed_plan_execute_locally = 1;

SELECT count() FROM test_lineitem RIGHT ANTI JOIN test_orders ON l_orderkey = o_orderkey
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

-- RIGHT ALL: not commutable, broadcast guard must block it.
-- Without the guard, the 20-row right side would be broadcast and unmatched
-- right rows would be duplicated across 4 nodes.
SELECT '-- RIGHT JOIN (RIGHT ALL, not commutable, broadcast blocked)';
EXPLAIN PLAN keep_logical_steps = 1
SELECT count() FROM test_orders RIGHT JOIN test_lineitem ON o_orderkey = l_orderkey;

SELECT count() FROM test_orders RIGHT JOIN test_lineitem ON o_orderkey = l_orderkey
SETTINGS distributed_plan_execute_locally = 1;

SELECT count() FROM test_orders RIGHT JOIN test_lineitem ON o_orderkey = l_orderkey
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

-- FULL ALL: not commutable, broadcast guard must block it.
-- Both sides produce unmatched rows; broadcasting either side would duplicate them.
SELECT '-- FULL JOIN (FULL ALL, not commutable, broadcast blocked)';
EXPLAIN PLAN keep_logical_steps = 1
SELECT count() FROM test_orders FULL JOIN test_lineitem ON o_orderkey = l_orderkey;

SELECT count() FROM test_orders FULL JOIN test_lineitem ON o_orderkey = l_orderkey
SETTINGS distributed_plan_execute_locally = 1;

SELECT count() FROM test_orders FULL JOIN test_lineitem ON o_orderkey = l_orderkey
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

DROP TABLE test_orders;
DROP TABLE test_lineitem;
