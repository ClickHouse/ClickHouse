-- Test for ORDER BY pushdown optimization into VIEWs over distributed tables
-- This ensures that querying a VIEW with ORDER BY uses merge sorted streams
-- instead of full sort on coordinator.

-- Tags: distributed

SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS test_local_03788;
DROP TABLE IF EXISTS test_distributed_03788;
DROP VIEW IF EXISTS test_view_03788;

-- Create local table
CREATE TABLE test_local_03788 (
    id UInt64,
    val String,
    ts DateTime
) ENGINE = MergeTree() 
ORDER BY (id, ts);

-- Create distributed table over 2 localhost shards
CREATE TABLE test_distributed_03788 AS test_local_03788
ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), test_local_03788, id);

-- Insert test data
INSERT INTO test_local_03788 SELECT number, toString(number), now() - number FROM numbers(100);

-- Create simple view over distributed table
CREATE VIEW test_view_03788 AS
SELECT id, val, ts FROM test_distributed_03788;

-- Test: Both direct and VIEW queries should use "Merge sorted streams" optimization
-- The VIEW is transparent for query processing stage, so it delegates to the
-- underlying Distributed table, enabling the merge sorted streams optimization.

-- Verify both direct query and view query have the merge sorted streams optimization
SELECT 'Direct query has merge sort:',
    (SELECT count() > 0 FROM (EXPLAIN SELECT id FROM test_distributed_03788 ORDER BY ts DESC LIMIT 10) 
     WHERE explain LIKE '%Merge sorted streams%') AS has_merge_sort;

SELECT 'View query has merge sort:',
    (SELECT count() > 0 FROM (EXPLAIN SELECT id FROM test_view_03788 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS has_merge_sort;

-- Cleanup
DROP VIEW test_view_03788;
DROP TABLE test_distributed_03788;
DROP TABLE test_local_03788;
