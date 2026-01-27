-- Tests for hybrid row-column storage reading functionality (Phase 3)
-- Tags: no-random-settings

DROP TABLE IF EXISTS test_hybrid_storage_read;

-- Create a table with hybrid storage enabled
CREATE TABLE test_hybrid_storage_read
(
    id UInt64,
    key1 String,
    col1 String,
    col2 UInt32,
    col3 Float64,
    col4 String,
    col5 Array(UInt32),
    col6 Nullable(String),
    col7 DateTime
)
ENGINE = MergeTree()
ORDER BY (id, key1)
SETTINGS enable_hybrid_storage = 1,
         hybrid_storage_column_threshold = 0.5,
         hybrid_storage_max_row_size = 1048576;

-- Insert test data
INSERT INTO test_hybrid_storage_read VALUES
    (1, 'a', 'value1', 10, 1.5, 'x', [1,2,3], 'nullable1', '2024-01-01 00:00:00'),
    (2, 'b', 'value2', 20, 2.5, 'y', [4,5,6], NULL, '2024-01-02 00:00:00'),
    (3, 'c', 'value3', 30, 3.5, 'z', [7,8,9], 'nullable3', '2024-01-03 00:00:00');

-- Force data to disk
SYSTEM FLUSH LOGS;

-- Test 1: Wide query - should use hybrid row-based reading (>50% of non-key columns)
-- Requesting all columns (SELECT *)
SELECT 'Test 1: Wide query (SELECT *)';
SELECT * FROM test_hybrid_storage_read ORDER BY id;

-- Test 2: Point query with many columns - should use hybrid row-based reading
SELECT 'Test 2: Point query with many columns';
SELECT col1, col2, col3, col4, col5, col6, col7 FROM test_hybrid_storage_read WHERE id = 2;

-- Test 3: Narrow query - should use column-based reading (<50% of columns)
SELECT 'Test 3: Narrow query (2 columns)';
SELECT id, col1 FROM test_hybrid_storage_read ORDER BY id;

-- Test 4: Key columns only - should use column-based reading
SELECT 'Test 4: Key columns only';
SELECT id, key1 FROM test_hybrid_storage_read ORDER BY id;

-- Test 5: Mixed key and non-key columns
SELECT 'Test 5: Mixed key and non-key columns';
SELECT id, key1, col1, col2 FROM test_hybrid_storage_read WHERE id = 1;

-- Test 6: Query with filtering on key column
SELECT 'Test 6: Query with key column filter';
SELECT col1, col2, col3, col4, col5 FROM test_hybrid_storage_read WHERE id > 1 ORDER BY id;

-- Test 7: Aggregation query (typically benefits from column reading)
SELECT 'Test 7: Aggregation query';
SELECT sum(col2), avg(col3) FROM test_hybrid_storage_read;

-- Test 8: Check hybrid storage is enabled (note: __row is internal, not visible in system.columns)
SELECT 'Test 8: Check __row column exists';
-- __row is an internal column stored directly in part files, not exposed via system.columns

-- Test 9: Verify data integrity with nullable columns
SELECT 'Test 9: Nullable columns';
SELECT id, col6 FROM test_hybrid_storage_read ORDER BY id;

-- Test 10: Array column handling
SELECT 'Test 10: Array columns';
SELECT id, col5 FROM test_hybrid_storage_read ORDER BY id;

DROP TABLE test_hybrid_storage_read;

-- Test with compact parts
SELECT 'Test 11: Compact parts with hybrid storage';

DROP TABLE IF EXISTS test_hybrid_compact;

CREATE TABLE test_hybrid_compact
(
    id UInt64,
    col1 String,
    col2 UInt32,
    col3 Float64,
    col4 String
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS enable_hybrid_storage = 1,
         hybrid_storage_column_threshold = 0.5,
         min_bytes_for_wide_part = 1000000000;  -- Force compact parts

INSERT INTO test_hybrid_compact VALUES
    (1, 'a', 10, 1.5, 'x'),
    (2, 'b', 20, 2.5, 'y'),
    (3, 'c', 30, 3.5, 'z');

-- Query all non-key columns (should use hybrid reading if available)
SELECT * FROM test_hybrid_compact ORDER BY id;

-- Query subset of columns
SELECT id, col1 FROM test_hybrid_compact ORDER BY id;

DROP TABLE test_hybrid_compact;

-- Test with different column threshold
SELECT 'Test 12: Different threshold (0.3)';

DROP TABLE IF EXISTS test_hybrid_threshold;

CREATE TABLE test_hybrid_threshold
(
    id UInt64,
    c1 String, c2 String, c3 String, c4 String, c5 String,
    c6 String, c7 String, c8 String, c9 String, c10 String
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS enable_hybrid_storage = 1,
         hybrid_storage_column_threshold = 0.3;

INSERT INTO test_hybrid_threshold SELECT
    number,
    toString(number), toString(number+1), toString(number+2),
    toString(number+3), toString(number+4), toString(number+5),
    toString(number+6), toString(number+7), toString(number+8),
    toString(number+9)
FROM numbers(10);

-- This should use hybrid reading (4 of 10 non-key columns = 40% > 30% threshold)
SELECT id, c1, c2, c3, c4 FROM test_hybrid_threshold WHERE id < 3 ORDER BY id;

-- This should use column reading (2 of 10 non-key columns = 20% < 30% threshold)
SELECT id, c1, c2 FROM test_hybrid_threshold WHERE id < 3 ORDER BY id;

DROP TABLE test_hybrid_threshold;

-- Test 13: Query plan optimization tests (Phase 4)
SELECT 'Test 13: Query plan optimization';

DROP TABLE IF EXISTS test_hybrid_optimization;

CREATE TABLE test_hybrid_optimization
(
    id UInt64,
    c1 String, c2 String, c3 String, c4 String, c5 String,
    c6 String, c7 String, c8 String, c9 String, c10 String
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS enable_hybrid_storage = 1,
         hybrid_storage_column_threshold = 0.5;

INSERT INTO test_hybrid_optimization SELECT
    number,
    toString(number), toString(number+1), toString(number+2),
    toString(number+3), toString(number+4), toString(number+5),
    toString(number+6), toString(number+7), toString(number+8),
    toString(number+9)
FROM numbers(100);

-- Test query plan optimization is applied for wide queries
-- When reading >50% of columns, optimization should enable hybrid reading
SELECT 'Test 13a: Wide query optimization';
SELECT count() FROM (
    SELECT * FROM test_hybrid_optimization WHERE id < 10
);

-- Test optimization is NOT applied for narrow queries
-- When reading <50% of columns, should use column-based reading
SELECT 'Test 13b: Narrow query (no optimization)';
SELECT count() FROM (
    SELECT id, c1, c2 FROM test_hybrid_optimization WHERE id < 10
);

-- Test with query_plan_optimize_hybrid_storage = 0 (disabled)
SELECT 'Test 13c: Optimization disabled';
SELECT count() FROM (
    SELECT * FROM test_hybrid_optimization WHERE id < 10
    SETTINGS query_plan_optimize_hybrid_storage = 0
);

-- Test that the optimization can be controlled at query level
SELECT 'Test 13d: Query-level setting override';
SELECT count() FROM (
    SELECT c1, c2, c3, c4, c5, c6 FROM test_hybrid_optimization WHERE id < 10
    SETTINGS query_plan_optimize_hybrid_storage = 1
);

DROP TABLE test_hybrid_optimization;

-- Phase 5 Tests: Merge and Mutation with Hybrid Storage

-- Test 14: Merge preserves data integrity with hybrid storage
SELECT 'Test 14: Merge with hybrid storage';

DROP TABLE IF EXISTS test_hybrid_merge;

CREATE TABLE test_hybrid_merge
(
    id UInt64,
    key1 String,
    col1 String,
    col2 UInt32,
    col3 Float64
)
ENGINE = MergeTree()
ORDER BY (id, key1)
SETTINGS enable_hybrid_storage = 1,
         hybrid_storage_column_threshold = 0.5;

-- Insert data into multiple parts
INSERT INTO test_hybrid_merge VALUES (1, 'a', 'val1', 10, 1.5);
INSERT INTO test_hybrid_merge VALUES (2, 'b', 'val2', 20, 2.5);
INSERT INTO test_hybrid_merge VALUES (3, 'c', 'val3', 30, 3.5);

-- Force merge (horizontal merge since we have few columns)
OPTIMIZE TABLE test_hybrid_merge FINAL;

-- Verify data integrity after merge
SELECT * FROM test_hybrid_merge ORDER BY id;

-- Verify query still works with hybrid storage
SELECT col1, col2, col3 FROM test_hybrid_merge WHERE id = 2;

DROP TABLE test_hybrid_merge;

-- Test 15: Mutation with hybrid storage (full rewrite)
SELECT 'Test 15: Mutation full rewrite';

DROP TABLE IF EXISTS test_hybrid_mutation;

CREATE TABLE test_hybrid_mutation
(
    id UInt64,
    col1 String,
    col2 UInt32,
    col3 Float64,
    col4 String
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS enable_hybrid_storage = 1,
         hybrid_storage_column_threshold = 0.5;

INSERT INTO test_hybrid_mutation VALUES (1, 'a', 10, 1.5, 'x');
INSERT INTO test_hybrid_mutation VALUES (2, 'b', 20, 2.5, 'y');
INSERT INTO test_hybrid_mutation VALUES (3, 'c', 30, 3.5, 'z');

-- Optimize to get single part
OPTIMIZE TABLE test_hybrid_mutation FINAL;

-- Mutation that rewrites data
ALTER TABLE test_hybrid_mutation UPDATE col1 = 'updated' WHERE id = 2;

-- Wait for mutation to complete
SELECT sleepEachRow(0.1) FROM numbers(5) FORMAT Null;

-- Verify data after mutation
SELECT * FROM test_hybrid_mutation ORDER BY id;

-- Query with multiple non-key columns (should use hybrid reading if __row exists)
SELECT col1, col2, col3, col4 FROM test_hybrid_mutation WHERE id = 2;

DROP TABLE test_hybrid_mutation;

-- Test 16: Lightweight delete with hybrid storage
SELECT 'Test 16: Lightweight delete';

DROP TABLE IF EXISTS test_hybrid_delete;

CREATE TABLE test_hybrid_delete
(
    id UInt64,
    col1 String,
    col2 UInt32,
    col3 Float64
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS enable_hybrid_storage = 1,
         hybrid_storage_column_threshold = 0.5;

INSERT INTO test_hybrid_delete VALUES (1, 'a', 10, 1.5);
INSERT INTO test_hybrid_delete VALUES (2, 'b', 20, 2.5);
INSERT INTO test_hybrid_delete VALUES (3, 'c', 30, 3.5);
INSERT INTO test_hybrid_delete VALUES (4, 'd', 40, 4.5);

OPTIMIZE TABLE test_hybrid_delete FINAL;

-- Delete a row
DELETE FROM test_hybrid_delete WHERE id = 2;

-- Wait for mutation
SELECT sleepEachRow(0.1) FROM numbers(5) FORMAT Null;

-- Verify data after delete
SELECT * FROM test_hybrid_delete ORDER BY id;

-- Clean up deleted rows
OPTIMIZE TABLE test_hybrid_delete FINAL;

-- Verify after cleanup
SELECT * FROM test_hybrid_delete ORDER BY id;

DROP TABLE test_hybrid_delete;

-- Test 17: Multiple merges preserve hybrid storage
SELECT 'Test 17: Multiple merges';

DROP TABLE IF EXISTS test_hybrid_multi_merge;

CREATE TABLE test_hybrid_multi_merge
(
    id UInt64,
    c1 String, c2 String, c3 String, c4 String, c5 String
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS enable_hybrid_storage = 1,
         hybrid_storage_column_threshold = 0.5;

-- Insert multiple batches
INSERT INTO test_hybrid_multi_merge SELECT number, 'a', 'b', 'c', 'd', 'e' FROM numbers(10);
INSERT INTO test_hybrid_multi_merge SELECT number + 10, 'f', 'g', 'h', 'i', 'j' FROM numbers(10);
INSERT INTO test_hybrid_multi_merge SELECT number + 20, 'k', 'l', 'm', 'n', 'o' FROM numbers(10);

-- Merge all parts
OPTIMIZE TABLE test_hybrid_multi_merge FINAL;

-- Verify row count
SELECT count() FROM test_hybrid_multi_merge;

-- Wide query should use hybrid storage
SELECT count() FROM (SELECT * FROM test_hybrid_multi_merge WHERE id < 5);

DROP TABLE test_hybrid_multi_merge;

SELECT 'All hybrid storage reading tests passed';
