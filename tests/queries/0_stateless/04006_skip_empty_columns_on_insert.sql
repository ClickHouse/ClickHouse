-- Test: Skip writing all-default columns during MergeTree INSERT.
-- Columns whose values are entirely type-defaults are removed from the part
-- at INSERT time, saving disk space. The data reads back correctly because
-- missing columns are filled with defaults (same as ALTER ADD COLUMN).

-- Disable sparse serialization so it doesn't interfere with our optimization.
-- Disable virtual columns that could add extra columns to the part.
SET mutations_sync = 2;

-- ============================================================================
-- CASE 1: All-default columns are not written to the part.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_cols;

CREATE TABLE t_skip_empty_cols
(
    key UInt64,
    val1 UInt64,
    val2 String,
    val3 Float64
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- val1=100, val2='' (default), val3=0.0 (default) → val2, val3 should be skipped
INSERT INTO t_skip_empty_cols (key, val1, val2, val3) VALUES (1, 100, '', 0);

SELECT 'case1_columns_in_part';
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_cols' AND active
ORDER BY column;

-- Data reads back correctly (defaults filled)
SELECT 'case1_data';
SELECT * FROM t_skip_empty_cols ORDER BY key;

DROP TABLE t_skip_empty_cols;

-- ============================================================================
-- CASE 2: All columns are default → at least one column must remain.
-- We cannot write a part with zero columns.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_all_default;

CREATE TABLE t_skip_empty_all_default
(
    key UInt64,
    val UInt64
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- Both key=0 and val=0 are defaults → should NOT remove all columns
INSERT INTO t_skip_empty_all_default (key, val) VALUES (0, 0);

SELECT 'case2_data';
SELECT * FROM t_skip_empty_all_default ORDER BY key;

DROP TABLE t_skip_empty_all_default;

-- ============================================================================
-- CASE 3: Merge works correctly with parts that have missing columns.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_merge;

CREATE TABLE t_skip_empty_merge
(
    key UInt64,
    a UInt64,
    b UInt64,
    c String
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- Part 1: a=100, b=0 (default), c='' (default)
INSERT INTO t_skip_empty_merge (key, a, b, c) VALUES (1, 100, 0, '');
-- Part 2: a=0 (default), b=200, c='' (default)
INSERT INTO t_skip_empty_merge (key, a, b, c) VALUES (2, 0, 200, '');
-- Part 3: a=0 (default), b=0 (default), c='hello'
INSERT INTO t_skip_empty_merge (key, a, b, c) VALUES (3, 0, 0, 'hello');

SELECT 'case3_pre_merge';
SELECT * FROM t_skip_empty_merge ORDER BY key;

OPTIMIZE TABLE t_skip_empty_merge FINAL;

SELECT 'case3_post_merge';
SELECT * FROM t_skip_empty_merge ORDER BY key;

DROP TABLE t_skip_empty_merge;

-- ============================================================================
-- CASE 4: Mutation works correctly with parts that have missing columns.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_mutate;

CREATE TABLE t_skip_empty_mutate
(
    key UInt64,
    a UInt64,
    b String
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- b='' (default) → b should be skipped
INSERT INTO t_skip_empty_mutate (key, a, b) VALUES (1, 100, '');

SELECT 'case4_pre_mutate';
SELECT * FROM t_skip_empty_mutate ORDER BY key;

-- Mutate: set b to a non-default value
ALTER TABLE t_skip_empty_mutate UPDATE b = 'mutated' WHERE key = 1;

SELECT 'case4_post_mutate';
SELECT * FROM t_skip_empty_mutate ORDER BY key;

DROP TABLE t_skip_empty_mutate;

-- ============================================================================
-- CASE 5: Nullable columns — NULL is the type default for Nullable.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_nullable;

CREATE TABLE t_skip_empty_nullable
(
    key UInt64,
    val Nullable(UInt64),
    val2 Nullable(String)
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- Both nullable cols are NULL (default) → should be skipped
INSERT INTO t_skip_empty_nullable (key, val, val2) VALUES (1, NULL, NULL);

SELECT 'case5_columns_in_part';
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_nullable' AND active
ORDER BY column;

-- Data reads back correctly
SELECT 'case5_data';
SELECT * FROM t_skip_empty_nullable ORDER BY key;

-- Insert with non-default values, merge, verify
INSERT INTO t_skip_empty_nullable (key, val, val2) VALUES (2, 42, 'hello');

OPTIMIZE TABLE t_skip_empty_nullable FINAL;

SELECT 'case5_post_merge';
SELECT * FROM t_skip_empty_nullable ORDER BY key;

DROP TABLE t_skip_empty_nullable;

-- ============================================================================
-- CASE 6: Key columns CAN be skipped (primary.idx has values baked in).
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_key;

CREATE TABLE t_skip_empty_key
(
    key1 UInt64,
    key2 UInt64,
    val UInt64
)
ENGINE = MergeTree
ORDER BY (key1, key2)
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- key2=0 (default), val=0 (default) → both should be skipped
INSERT INTO t_skip_empty_key (key1, key2, val) VALUES (1, 0, 0);

SELECT 'case6_columns_in_part';
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_key' AND active
ORDER BY column;

SELECT 'case6_data';
SELECT * FROM t_skip_empty_key ORDER BY key1;

DROP TABLE t_skip_empty_key;
