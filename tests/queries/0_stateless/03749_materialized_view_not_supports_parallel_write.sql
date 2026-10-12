-- Tags: no-asan, no-tsan, no-msan, no-ubsan, no-sanitize-coverage, no-parallel-replicas, no-flaky-check

SET parallel_view_processing = 1, max_insert_threads = 2;
-- Pin off: a ratio above 1.0 adds a ShrinkColumnsTransform to the insert chain,
-- which shows up in the EXPLAIN PIPELINE output below.
SET shrink_over_allocated_columns_min_waste_ratio = 1.0;

CREATE TABLE test_set (c0 Int) ENGINE = Set;
CREATE TABLE test_table (c0 Int) ENGINE = MergeTree ORDER BY c0 PARTITION BY c0;
CREATE MATERIALIZED VIEW merge_tree_to_set TO test_set (c0 Int) AS (SELECT * FROM test_table);
-- Expect the single insert chain
EXPLAIN PIPELINE INSERT INTO TABLE test_table SELECT 1 FROM numbers(10);

-- Fuzzed
CREATE TABLE t0 (c0 Int) ENGINE = Log;
CREATE TABLE t1 (c0 Int) ENGINE = Memory;
CREATE MATERIALIZED VIEW v0 TO t0 (c0 Int) AS (SELECT t1.* IS NULL c0 FROM t1);
INSERT INTO TABLE t1 (c0) SELECT c0 FROM generateRandom('c0 Int', 1, 1, 0) LIMIT 1;
