-- Test for fix: read-in-order optimization when ORDER BY includes a fixed (constant) column
-- Bug: When ORDER BY includes a column that is fixed via WHERE clause, the direction
-- from ORDER BY was incorrectly used, preventing InReverseOrder optimization.
-- Example: WHERE tenant = '42' ORDER BY tenant, event_time DESC should use InReverseOrder

SET optimize_read_in_order = 1;
SET read_in_order_use_virtual_row = 0;

DROP TABLE IF EXISTS t_fixed_key_direction;

CREATE TABLE t_fixed_key_direction (
    tenant String,
    event_time UInt64,
    id UInt64
)
ENGINE = MergeTree
ORDER BY (tenant, event_time, id);

INSERT INTO t_fixed_key_direction
SELECT
    toString(number % 10) AS tenant,
    number % 1000 AS event_time,
    number AS id
FROM numbers(10000);

-- Case 1: ORDER BY without fixed column in ORDER BY clause - should use InReverseOrder
SELECT '=== Case 1: WHERE tenant = 5 ORDER BY event_time DESC (baseline - should be InReverseOrder) ===';
EXPLAIN PIPELINE SELECT * FROM t_fixed_key_direction WHERE tenant = '5' ORDER BY event_time DESC LIMIT 5;

-- Case 2: ORDER BY includes fixed column - BEFORE FIX this would NOT use InReverseOrder
-- AFTER FIX this should also use InReverseOrder
SELECT '=== Case 2: WHERE tenant = 5 ORDER BY tenant, event_time DESC (fixed column in ORDER BY) ===';
EXPLAIN PIPELINE SELECT * FROM t_fixed_key_direction WHERE tenant = '5' ORDER BY tenant, event_time DESC LIMIT 5;

-- Case 3: ORDER BY includes fixed column with explicit ASC - should still use InReverseOrder
SELECT '=== Case 3: WHERE tenant = 5 ORDER BY tenant ASC, event_time DESC ===';
EXPLAIN PIPELINE SELECT * FROM t_fixed_key_direction WHERE tenant = '5' ORDER BY tenant ASC, event_time DESC LIMIT 5;

-- Case 4: ORDER BY includes fixed column with DESC - should still use InReverseOrder
SELECT '=== Case 4: WHERE tenant = 5 ORDER BY tenant DESC, event_time DESC ===';
EXPLAIN PIPELINE SELECT * FROM t_fixed_key_direction WHERE tenant = '5' ORDER BY tenant DESC, event_time DESC LIMIT 5;

-- Verify results are correct (should show highest event_time first)
SELECT '=== Verify results ===';
SELECT tenant, event_time FROM t_fixed_key_direction WHERE tenant = '5' ORDER BY tenant, event_time DESC LIMIT 3;

DROP TABLE t_fixed_key_direction;
