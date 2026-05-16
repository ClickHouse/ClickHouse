-- Test for ORDER BY pushdown optimization into VIEWs over distributed tables
-- This ensures that querying a VIEW with ORDER BY uses merge sorted streams
-- instead of full sort on coordinator.

-- Tags: distributed

SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS test_local_04241;
DROP TABLE IF EXISTS test_distributed_04241;
DROP VIEW IF EXISTS test_view_04241;

-- Create local table
CREATE TABLE test_local_04241 (
    id UInt64,
    val String,
    ts DateTime
) ENGINE = MergeTree()
ORDER BY (id, ts);

-- Create distributed table over 2 localhost shards
CREATE TABLE test_distributed_04241 AS test_local_04241
ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), test_local_04241, id);

-- Insert test data
INSERT INTO test_local_04241 SELECT number, toString(number), now() - number FROM numbers(100);

-- Create simple view over distributed table
CREATE VIEW test_view_04241 AS
SELECT id, val, ts FROM test_distributed_04241;

-- The VIEW must be transparent for ORDER BY semantics so the underlying
-- Distributed table can enable the merge-sorted-streams optimization.

SELECT 'Direct query has merge sort:',
    (SELECT count() > 0 FROM (EXPLAIN SELECT id FROM test_distributed_04241 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS has_merge_sort;

SELECT 'View query has merge sort:',
    (SELECT count() > 0 FROM (EXPLAIN SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS has_merge_sort;

-- View query must not perform a full coordinator-side sort.
SELECT 'View query avoids full sort on coordinator:',
    (SELECT countIf(explain LIKE '%Sorting (Sorting for ORDER BY)%') = 0
     FROM (EXPLAIN SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 10)) AS avoids_full_sort;

-- Result correctness: pushdown must not change result rows.
SELECT 'View ORDER BY+LIMIT result count:', count() FROM (
    SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 10
);

-- Outer GROUP BY: pushdown must be disabled (it would otherwise break aggregate
-- projection matching and similar optimizations). The query must still produce
-- correct results.
SELECT 'GROUP BY result count:', count() FROM (
    SELECT id, count() FROM test_view_04241 GROUP BY id ORDER BY id LIMIT 5
);

-- Outer LIMIT ... OFFSET ...: pushdown must be disabled to preserve correctness.
SELECT 'OFFSET result count:', count() FROM (
    SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 5 OFFSET 3
);

-- Cleanup
DROP VIEW test_view_04241;
DROP TABLE test_distributed_04241;
DROP TABLE test_local_04241;
