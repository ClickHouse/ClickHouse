-- Tags: no-random-merge-tree-settings, no-parallel-replicas
-- Remove random settings since we want to test specific settings, different parts, hashing, etc.
-- no-parallel-replicas: EXPLAIN output differs with parallel replicas
-- Test that skip index file names are hashed when they exceed max_file_name_length
-- Tests both explicit and implicit indices with compact, and wide parts

SET enable_analyzer = 1; -- Old analyzer: EXPLAIN output differs

DROP TABLE IF EXISTS test_long_index_name;

-- Create table with a very long column name that will create a long implicit index name
-- and an explicit index with a very long name
-- Use settings to force different part types and enable filename hashing
CREATE TABLE test_long_index_name (
    `一个非常非常非常非常非常非常非常非常非常非常非常长的中文字符串` Int64,
    `another_long_column_name_that_will_create_a_very_long_implicit_index_file_name` Int64,
    `short_col` Int64,
    INDEX `explicit_index_with_an_extremely_long_name_that_exceeds_the_maximum_file_name_length` short_col TYPE minmax GRANULARITY 1
) ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    replace_long_file_name_to_hash = 1,
    max_file_name_length = 50,
    min_bytes_for_wide_part = 0,  -- Start with wide parts
    min_rows_for_wide_part = 0,
    index_granularity = 2,  -- Small granularity to ensure multiple granules
    add_minmax_index_for_numeric_columns=1;  -- Disable adaptive granularity to avoid flaky test results

-- Insert data in ONE part with multiple granules (index_granularity=2 means 2 rows per granule)
-- This creates clear non-overlapping ranges per granule so minmax can skip granules
-- Granule 1: Chinese [1-2], English [10-20], short_col [100-200]
-- Granule 2: Chinese [100-200], English [1000-2000], short_col [300-400]
-- Granule 3: Chinese [300-400], English [3000-4000], short_col [500-600]
-- Granule 4: Chinese [500-600], English [5000-6000], short_col [700-800]
INSERT INTO test_long_index_name VALUES
    (1, 10, 100), (2, 20, 200),
    (100, 1000, 300), (200, 2000, 400),
    (300, 3000, 500), (400, 4000, 600),
    (500, 5000, 700), (600, 6000, 800);

-- Verify indices exist and data is readable
SELECT '-- Indices created:';
SELECT name, type FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'test_long_index_name'
ORDER BY name;

SELECT '-- Initial data:';
SELECT * FROM test_long_index_name ORDER BY short_col;

-- Test that indices are actually used in queries and filter out granules BEFORE optimize
SELECT '-- EXPLAIN for explicit index query:';
EXPLAIN indexes = 1
SELECT * FROM test_long_index_name
WHERE short_col = 100;

SELECT '-- Result of explicit index query:';
SELECT * FROM test_long_index_name WHERE short_col = 100;

SELECT '-- EXPLAIN for implicit index on Chinese column:';
EXPLAIN indexes = 1
SELECT * FROM test_long_index_name
WHERE `一个非常非常非常非常非常非常非常非常非常非常非常长的中文字符串` = 1;

SELECT '-- Result of Chinese column query:';
SELECT * FROM test_long_index_name
WHERE `一个非常非常非常非常非常非常非常非常非常非常非常长的中文字符串` = 1;

SELECT '-- EXPLAIN for implicit index on long English column:';
EXPLAIN indexes = 1
SELECT * FROM test_long_index_name
WHERE another_long_column_name_that_will_create_a_very_long_implicit_index_file_name = 10;

SELECT '-- Result of English column query:';
SELECT * FROM test_long_index_name
WHERE another_long_column_name_that_will_create_a_very_long_implicit_index_file_name = 10;

SELECT '-- Verify column files are hashed (32 char SipHash128):';
SELECT
    column,
    filenames,
    length(filenames[1]) as filename_length,
    length(filenames[1]) = 32 as is_hashed
FROM system.parts_columns
WHERE database = currentDatabase()
    AND table = 'test_long_index_name'
    AND active
    AND column IN (
        '一个非常非常非常非常非常非常非常非常非常非常非常长的中文字符串',
        'another_long_column_name_that_will_create_a_very_long_implicit_index_file_name'
    )
ORDER BY column
LIMIT 2;

DROP TABLE test_long_index_name;

-- Test with different settings (attempt compact parts with high thresholds)
SELECT '-- Testing with different part type settings:';

DROP TABLE IF EXISTS test_long_index_compact;

CREATE TABLE test_long_index_compact (
    `一个非常非常非常非常非常非常非常非常非常非常非常长的中文字符串` Int64,
    `another_long_column_name_that_will_create_a_very_long_implicit_index_file_name` Int64,
    `short_col` Int64,
    INDEX `explicit_index_with_an_extremely_long_name_that_exceeds_the_maximum_file_name_length` short_col TYPE minmax GRANULARITY 1
) ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    replace_long_file_name_to_hash = 1,
    max_file_name_length = 50,
    min_rows_for_wide_part = 100000000,  -- High thresholds to attempt compact parts
    min_bytes_for_wide_part = 1000000000,
    index_granularity = 2,
    add_minmax_index_for_numeric_columns=1;

INSERT INTO test_long_index_compact VALUES
    (1, 10, 100), (2, 20, 200),
    (100, 1000, 300), (200, 2000, 400);

SELECT '-- Part properties with different settings:';
SELECT
    rows,
    data_compressed_bytes > 0 as has_data
FROM system.parts
WHERE database = currentDatabase()
    AND table = 'test_long_index_compact'
    AND active
ORDER BY name;

SELECT '-- EXPLAIN with explicit index (different settings):';
EXPLAIN indexes = 1
SELECT * FROM test_long_index_compact
WHERE short_col = 100;

SELECT '-- EXPLAIN with implicit index (different settings):';
EXPLAIN indexes = 1
SELECT * FROM test_long_index_compact
WHERE `一个非常非常非常非常非常非常非常非常非常非常非常长的中文字符串` = 1;

DROP TABLE test_long_index_compact;
