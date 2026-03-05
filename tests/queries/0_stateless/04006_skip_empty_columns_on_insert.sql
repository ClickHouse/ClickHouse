-- Test: Verify that parts with all-default columns merge correctly.
-- The removeEmptyColumnsFromPart mechanism (merge-time, Wide parts only)
-- removes column files for columns that are entirely type-defaults after merge.
-- This test validates data integrity through such merges.

-- ============================================================================
-- CASE 1: After merge, all-default columns are removed from Wide parts.
-- ============================================================================
DROP TABLE IF EXISTS t_empty_cols_merge;

CREATE TABLE t_empty_cols_merge
(
    key UInt64,
    val1 UInt64,
    val2 String,
    val3 Float64
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_block_number_column = 0, enable_block_offset_column = 0;

-- Part 1: val1=100, val2='' (default), val3=0 (default)
INSERT INTO t_empty_cols_merge (key, val1, val2, val3) VALUES (1, 100, '', 0);
-- Part 2: val1=0 (default), val2='' (default), val3=0 (default)
INSERT INTO t_empty_cols_merge (key, val1, val2, val3) VALUES (2, 0, '', 0);

SELECT 'case1_pre_merge';
SELECT * FROM t_empty_cols_merge ORDER BY key;

OPTIMIZE TABLE t_empty_cols_merge FINAL;

SELECT 'case1_post_merge';
SELECT * FROM t_empty_cols_merge ORDER BY key;

DROP TABLE t_empty_cols_merge;

-- ============================================================================
-- CASE 2: Mixed default/non-default columns across parts — merge preserves data.
-- ============================================================================
DROP TABLE IF EXISTS t_empty_cols_mixed;

CREATE TABLE t_empty_cols_mixed
(
    key UInt64,
    a UInt64,
    b UInt64,
    c String
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_block_number_column = 0, enable_block_offset_column = 0;

-- Part 1: a=100, b=0 (default), c='' (default)
INSERT INTO t_empty_cols_mixed (key, a, b, c) VALUES (1, 100, 0, '');
-- Part 2: a=0 (default), b=200, c='' (default)
INSERT INTO t_empty_cols_mixed (key, a, b, c) VALUES (2, 0, 200, '');
-- Part 3: a=0 (default), b=0 (default), c='hello'
INSERT INTO t_empty_cols_mixed (key, a, b, c) VALUES (3, 0, 0, 'hello');

SELECT 'case2_pre_merge';
SELECT * FROM t_empty_cols_mixed ORDER BY key;

OPTIMIZE TABLE t_empty_cols_mixed FINAL;

SELECT 'case2_post_merge';
SELECT * FROM t_empty_cols_mixed ORDER BY key;

DROP TABLE t_empty_cols_mixed;

-- ============================================================================
-- CASE 3: Nullable columns — NULL is the type default for Nullable.
-- ============================================================================
DROP TABLE IF EXISTS t_empty_cols_nullable;

CREATE TABLE t_empty_cols_nullable
(
    key UInt64,
    val Nullable(UInt64),
    val2 Nullable(String)
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_empty_cols_nullable (key, val, val2) VALUES (1, NULL, NULL);
INSERT INTO t_empty_cols_nullable (key, val, val2) VALUES (2, 42, 'hello');

SELECT 'case3_pre_merge';
SELECT * FROM t_empty_cols_nullable ORDER BY key;

OPTIMIZE TABLE t_empty_cols_nullable FINAL;

SELECT 'case3_post_merge';
SELECT * FROM t_empty_cols_nullable ORDER BY key;

DROP TABLE t_empty_cols_nullable;
