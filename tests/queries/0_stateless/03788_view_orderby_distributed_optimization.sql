-- Tags: distributed

-- Test for ORDER BY pushdown optimization into simple VIEWs over distributed tables
-- This ensures that querying a VIEW with ORDER BY uses the same efficient "merge sorted streams"
-- optimization as querying the distributed table directly.

SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS test_local;
DROP TABLE IF EXISTS test_distributed;
DROP VIEW IF EXISTS test_simple_view;
DROP VIEW IF EXISTS test_view_with_orderby;
DROP VIEW IF EXISTS test_view_with_groupby;

-- Create local table
CREATE TABLE test_local (
    id UInt64,
    val String
) ENGINE = Memory;

INSERT INTO test_local SELECT number, toString(number) FROM numbers(100);

-- Create distributed table (using remote function for testing)
CREATE TABLE test_distributed AS test_local ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), test_local);

-- Create simple view (just projection, should get ORDER BY pushdown)
CREATE VIEW test_simple_view AS
SELECT id, val FROM test_distributed;

-- Test 1: Query through simple view with ORDER BY
-- Should show "Merge sorted streams for ORDER BY" optimization
SELECT '=== Test 1: Simple view with ORDER BY (should use merge sort) ===';
EXPLAIN SELECT * FROM test_simple_view ORDER BY id LIMIT 10
SETTINGS enable_analyzer=1;

-- Test 2: View with existing ORDER BY (should NOT inject outer ORDER BY)
CREATE VIEW test_view_with_orderby AS
SELECT id, val FROM test_distributed ORDER BY val;

SELECT '';
SELECT '=== Test 2: View with existing ORDER BY ===';
EXPLAIN SELECT * FROM test_view_with_orderby ORDER BY id LIMIT 10
SETTINGS enable_analyzer=1;

-- Test 3: View with GROUP BY (should NOT inject ORDER BY)
CREATE VIEW test_view_with_groupby AS
SELECT id, count(*) as cnt FROM test_distributed GROUP BY id;

SELECT '';
SELECT '=== Test 3: View with GROUP BY ===';
EXPLAIN SELECT * FROM test_view_with_groupby ORDER BY id LIMIT 10
SETTINGS enable_analyzer=1;

-- Cleanup
DROP VIEW test_simple_view;
DROP VIEW test_view_with_orderby;
DROP VIEW test_view_with_groupby;
DROP TABLE test_distributed;
DROP TABLE test_local;
