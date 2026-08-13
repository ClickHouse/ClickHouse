-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-random-settings, no-random-merge-tree-settings, no-parallel-replicas: Explain output may differ

-- { echo }

-- Permuted negative LIMIT BY: the global order is (g, x) but the query lists the keys reversed (x, g).
-- They still cover the sort prefix, so the streaming NegativeLimitBySortedStreamTransform is used.
DROP TABLE IF EXISTS test_neg_permuted;
CREATE TABLE test_neg_permuted (g UInt8, x UInt32, v UInt32) ENGINE = MergeTree ORDER BY (g, x);
INSERT INTO test_neg_permuted SELECT number % 3, number % 4, number FROM numbers(120);
SELECT DISTINCT trim(BOTH ' ' FROM explain) FROM (EXPLAIN PIPELINE SELECT g, x FROM test_neg_permuted ORDER BY g, x LIMIT -2 BY x, g) WHERE explain ILIKE '%NegativeLimitBy%';
SELECT (SELECT groupArray((g, x)) FROM (SELECT g, x FROM test_neg_permuted ORDER BY g, x LIMIT -2 BY x, g SETTINGS query_plan_enable_optimizations = 0)) = (SELECT groupArray((g, x)) FROM (SELECT g, x FROM test_neg_permuted ORDER BY g, x LIMIT -2 BY x, g SETTINGS query_plan_enable_optimizations = 1));
DROP TABLE test_neg_permuted;

-- Permuted negative LIMIT BY with OFFSET: the kept-row count per group is independent of read order.
DROP TABLE IF EXISTS test_neg_permuted_offset;
CREATE TABLE test_neg_permuted_offset (g UInt8, x UInt32, v UInt32) ENGINE = MergeTree ORDER BY (g, x);
INSERT INTO test_neg_permuted_offset SELECT number % 3, number % 4, number FROM numbers(120);
SELECT DISTINCT trim(BOTH ' ' FROM explain) FROM (EXPLAIN PIPELINE SELECT g, x FROM test_neg_permuted_offset ORDER BY g, x LIMIT -2 OFFSET -1 BY x, g) WHERE explain ILIKE '%NegativeLimitBy%';
SELECT (SELECT count() FROM (SELECT g, x FROM test_neg_permuted_offset ORDER BY g, x LIMIT -2 OFFSET -1 BY x, g SETTINGS query_plan_enable_optimizations = 0)) = (SELECT count() FROM (SELECT g, x FROM test_neg_permuted_offset ORDER BY g, x LIMIT -2 OFFSET -1 BY x, g SETTINGS query_plan_enable_optimizations = 1));
DROP TABLE test_neg_permuted_offset;

-- Partial cover (LIMIT BY g while the order is only (x)) keeps the hash NegativeLimitByTransform.
DROP TABLE IF EXISTS test_neg_partial;
CREATE TABLE test_neg_partial (g UInt8, x UInt32, v UInt32) ENGINE = MergeTree ORDER BY (g, x);
INSERT INTO test_neg_partial SELECT number % 3, number % 4, number FROM numbers(120);
SELECT DISTINCT trim(BOTH ' ' FROM explain) FROM (EXPLAIN PIPELINE SELECT g, x FROM test_neg_partial ORDER BY x LIMIT -2 BY g) WHERE explain ILIKE '%NegativeLimitBy%';
DROP TABLE test_neg_partial;
