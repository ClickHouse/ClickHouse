-- Tags: no-random-merge-tree-settings

-- Test TieredDistributedMerge engine predicate filtering and virtual column functionality

DROP TABLE IF EXISTS test_table1_local SYNC;
DROP TABLE IF EXISTS test_table2_local SYNC;
DROP TABLE IF EXISTS test_tiered_predicate_filtering_analyzer_off SYNC;

-- Create local tables with data before and after watermark
CREATE TABLE test_table1_local
(
    `id` UInt32,
    `name` String,
    `date` Date,
    `value` UInt32
)
ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE test_table2_local
(
    `id` UInt32,
    `name` String,
    `date` Date,
    `value` UInt32
)
ENGINE = MergeTree()
ORDER BY id;

-- Insert data before watermark (2025-09-01)
INSERT INTO test_table1_local VALUES
    (11, 'Alice', '2025-08-15', 100),
    (12, 'Bob', '2025-08-20', 200),
    (13, 'Charlie', '2025-08-25', 300);

INSERT INTO test_table2_local VALUES
    (21, 'Alice', '2025-08-15', 100),
    (22, 'Bob', '2025-08-20', 200),
    (23, 'Charlie', '2025-08-25', 300);

-- Insert data after watermark (2025-09-01)
INSERT INTO test_table1_local VALUES
    (14, 'David', '2025-09-05', 400),
    (15, 'Eve', '2025-09-10', 500),
    (16, 'Frank', '2025-09-15', 600);

INSERT INTO test_table2_local VALUES
    (24, 'David', '2025-09-05', 400),
    (25, 'Eve', '2025-09-10', 500),
    (26, 'Frank', '2025-09-15', 600);

-- Test 1: Basic predicate filtering with analyzer disabled
SET allow_experimental_analyzer = 1;

CREATE TABLE test_tiered_predicate_filtering_analyzer_off
(
    `id` UInt32,
    `name` String,
    `date` Date,
    `value` UInt32
)
ENGINE = TieredDistributedMerge(
    remote('127.0.0.2:9000', currentDatabase(), 'test_table1_local'),
    date >= '2025-09-01',
    remote('127.0.0.2:9000', currentDatabase(), 'test_table2_local'),
    date < '2025-09-01'
);

-- Test predicate filtering - should return only data after watermark from table1 and before watermark from table2
SELECT 'Test 1: Predicate filtering with analyzer OFF' as test_name;
SELECT * FROM test_tiered_predicate_filtering_analyzer_off ORDER BY id;
SELECT count() as total_rows FROM test_tiered_predicate_filtering_analyzer_off;
SELECT count() as rows_after_watermark FROM test_tiered_predicate_filtering_analyzer_off WHERE date >= '2025-09-01';
SELECT count() as rows_before_watermark FROM test_tiered_predicate_filtering_analyzer_off WHERE date < '2025-09-01';

-- Test virtual column functionality
SELECT 'Test 2: Virtual column _table_index with analyzer OFF' as test_name;
SELECT _table_index, count() as row_count FROM test_tiered_predicate_filtering_analyzer_off GROUP BY _table_index ORDER BY _table_index;

-- Test 3: Basic predicate filtering with analyzer enabled
SET allow_experimental_analyzer = 1;

CREATE TABLE test_tiered_predicate_filtering_analyzer_on
(
    `id` UInt32,
    `name` String,
    `date` Date,
    `value` UInt32
)
ENGINE = TieredDistributedMerge(
    remote('127.0.0.1:9000,127.0.0.2:9000', currentDatabase(), 'test_table1_local'),
    date >= '2025-09-01',
    remote('127.0.0.1:9000,127.0.0.2:9000', currentDatabase(), 'test_table2_local'),
    date < '2025-09-01'
);

-- Test predicate filtering - should return only data after watermark from table1 and before watermark from table2
SELECT 'Test 3: Predicate filtering with analyzer ON' as test_name;
SELECT count() as total_rows FROM test_tiered_predicate_filtering_analyzer_on;
SELECT count() as rows_after_watermark FROM test_tiered_predicate_filtering_analyzer_on WHERE date >= '2025-09-01';
SELECT count() as rows_before_watermark FROM test_tiered_predicate_filtering_analyzer_on WHERE date < '2025-09-01';

-- Test virtual column functionality
SELECT 'Test 4: Virtual column _table_index with analyzer ON' as test_name;
SELECT _table_index, count() as row_count FROM test_tiered_predicate_filtering_analyzer_on GROUP BY _table_index ORDER BY _table_index;

-- Test 5: Complex predicate with multiple conditions
SET allow_experimental_analyzer = 0;

CREATE TABLE test_tiered_complex_predicate
(
    `id` UInt32,
    `name` String,
    `date` Date,
    `value` UInt32
)
ENGINE = TieredDistributedMerge(
    remote('127.0.0.1:9000,127.0.0.2:9000', currentDatabase(), 'test_table1_local'),
    date >= '2025-09-01' AND value > 400,
    remote('127.0.0.1:9000,127.0.0.2:9000', currentDatabase(), 'test_table2_local'),
    date < '2025-09-01' AND value < 300
);

SELECT 'Test 5: Complex predicate filtering' as test_name;
SELECT count() as total_rows FROM test_tiered_complex_predicate;
SELECT _table_index, count() as row_count FROM test_tiered_complex_predicate GROUP BY _table_index ORDER BY _table_index;

-- Test 6: Verify data integrity - check specific values
SELECT 'Test 6: Data integrity check' as test_name;
SELECT _table_index, id, name, date, value FROM test_tiered_predicate_filtering_analyzer_off ORDER BY _table_index, id;

-- Test 7: Test with additional WHERE clause on top of engine predicates
SELECT 'Test 7: Additional WHERE clause' as test_name;
SELECT count() as alice_rows FROM test_tiered_predicate_filtering_analyzer_off WHERE name = 'Alice';
SELECT count() as high_value_rows FROM test_tiered_predicate_filtering_analyzer_off WHERE value > 300;

-- Test 8: Test with analyzer enabled and additional WHERE clause
SET allow_experimental_analyzer = 1;
SELECT 'Test 8: Additional WHERE clause with analyzer ON' as test_name;
SELECT count() as alice_rows FROM test_tiered_predicate_filtering_analyzer_on WHERE name = 'Alice';
SELECT count() as high_value_rows FROM test_tiered_predicate_filtering_analyzer_on WHERE value > 300;

-- Clean up
DROP TABLE IF EXISTS test_tiered_predicate_filtering_analyzer_off;
DROP TABLE IF EXISTS test_tiered_predicate_filtering_analyzer_on;
DROP TABLE IF EXISTS test_tiered_complex_predicate;
DROP TABLE IF EXISTS test_table1_local;
DROP TABLE IF EXISTS test_table2_local;
