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
         skip_empty_columns_on_insert = 1,
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
         skip_empty_columns_on_insert = 1,
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- Both key=0 and val=0 are defaults → one column (the smallest) is kept
INSERT INTO t_skip_empty_all_default (key, val) VALUES (0, 0);

SELECT 'case2_columns_in_part';
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_all_default' AND active
ORDER BY column;

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
         skip_empty_columns_on_insert = 1,
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
         skip_empty_columns_on_insert = 1,
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
         skip_empty_columns_on_insert = 1,
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
         skip_empty_columns_on_insert = 1,
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

-- ============================================================================
-- CASE 7: Columns with DEFAULT expressions must NOT be skipped.
-- Inserting explicit type-default (0) into a column with DEFAULT expr would
-- cause the read path to evaluate the expression instead of returning 0.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_default_expr;

CREATE TABLE t_skip_empty_default_expr
(
    key UInt64,
    a UInt64,
    b UInt64 DEFAULT a + 1,
    c UInt64 MATERIALIZED a * 10
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         skip_empty_columns_on_insert = 1,
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- Explicitly insert b=0 (type-default). b must NOT be skipped because it has
-- a DEFAULT expression — otherwise read would return a+1=6 instead of 0.
INSERT INTO t_skip_empty_default_expr (key, a, b) VALUES (1, 5, 0);

SELECT 'case7_columns_in_part';
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_default_expr' AND active
ORDER BY column;

SELECT 'case7_data';
SELECT key, a, b, c FROM t_skip_empty_default_expr ORDER BY key;

DROP TABLE t_skip_empty_default_expr;

-- ============================================================================
-- CASE 8: Array columns — empty array [] is the type-default.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_array;

CREATE TABLE t_skip_empty_array
(
    key UInt64,
    arr1 Array(UInt64),
    arr2 Array(String)
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         skip_empty_columns_on_insert = 1,
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- Both arrays are empty (default) → should be skipped
INSERT INTO t_skip_empty_array (key, arr1, arr2) VALUES (1, [], []);

SELECT 'case8_columns_in_part';
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_array' AND active
ORDER BY column;

SELECT 'case8_data';
SELECT * FROM t_skip_empty_array ORDER BY key;

-- Insert with non-default values, merge, verify
INSERT INTO t_skip_empty_array (key, arr1, arr2) VALUES (2, [10, 20], ['a', 'b']);

OPTIMIZE TABLE t_skip_empty_array FINAL;

SELECT 'case8_post_merge';
SELECT * FROM t_skip_empty_array ORDER BY key;

DROP TABLE t_skip_empty_array;

-- ============================================================================
-- CASE 9: Tuple columns — (0, '') is the type-default for Tuple(UInt64, String).
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_tuple;

CREATE TABLE t_skip_empty_tuple
(
    key UInt64,
    t1 Tuple(UInt64, String),
    t2 Tuple(a UInt64, b Float64)
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         skip_empty_columns_on_insert = 1,
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- Both tuples are type-defaults → should be skipped
INSERT INTO t_skip_empty_tuple (key, t1, t2) VALUES (1, (0, ''), (0, 0));

SELECT 'case9_columns_in_part';
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_tuple' AND active
ORDER BY column;

SELECT 'case9_data';
SELECT * FROM t_skip_empty_tuple ORDER BY key;

-- Insert with non-default values, merge, verify
INSERT INTO t_skip_empty_tuple (key, t1, t2) VALUES (2, (42, 'hello'), (7, 3.14));

OPTIMIZE TABLE t_skip_empty_tuple FINAL;

SELECT 'case9_post_merge';
SELECT * FROM t_skip_empty_tuple ORDER BY key;

DROP TABLE t_skip_empty_tuple;

-- ============================================================================
-- CASE 10: ColumnSparse — when the source table uses sparse serialization,
-- INSERT SELECT can pass ColumnSparse columns into the write path. The
-- `hasOnlyTypeDefaults` call in `skipEmptyColumnsOnInsert` must handle them.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_sparse_src;
DROP TABLE IF EXISTS t_skip_empty_sparse_dst;

-- Source table with a very low ratio so that column `val` gets sparse
-- serialization (most values are 0).
CREATE TABLE t_skip_empty_sparse_src
(
    key UInt64,
    val UInt64
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 0.1,
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- Insert enough rows with val=0 (default) so the column gets sparse encoding.
INSERT INTO t_skip_empty_sparse_src SELECT number, 0 FROM numbers(1000);

-- Verify the source part uses sparse serialization for `val`.
SELECT 'case10_src_serialization';
SELECT column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_sparse_src' AND active AND column = 'val';

-- Destination table with skip_empty_columns_on_insert enabled.
CREATE TABLE t_skip_empty_sparse_dst
(
    key UInt64,
    val UInt64
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         skip_empty_columns_on_insert = 1,
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- INSERT SELECT: the block coming from the source may contain ColumnSparse
-- for `val`. All values are 0 (type-default), so `val` should be skipped.
INSERT INTO t_skip_empty_sparse_dst SELECT * FROM t_skip_empty_sparse_src;

SELECT 'case10_columns_in_part';
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_sparse_dst' AND active
ORDER BY column;

SELECT 'case10_data_sample';
SELECT count(), sum(val) FROM t_skip_empty_sparse_dst;

DROP TABLE t_skip_empty_sparse_src;
DROP TABLE t_skip_empty_sparse_dst;

-- ============================================================================
-- CASE 11: Compact parts — all columns stored in a single file, but the
-- optimization should still skip type-default columns in columns.txt.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_compact;

CREATE TABLE t_skip_empty_compact
(
    key UInt64,
    a UInt64,
    b String,
    c Float64
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         skip_empty_columns_on_insert = 1,
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- a=100, b='' (default), c=0 (default) → b, c should be skipped
INSERT INTO t_skip_empty_compact (key, a, b, c) VALUES (1, 100, '', 0);

SELECT 'case11_columns_in_part';
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_compact' AND active
ORDER BY column;

SELECT 'case11_data';
SELECT * FROM t_skip_empty_compact ORDER BY key;

-- Merge compact parts
INSERT INTO t_skip_empty_compact (key, a, b, c) VALUES (2, 0, 'hello', 3.14);

OPTIMIZE TABLE t_skip_empty_compact FINAL;

SELECT 'case11_post_merge';
SELECT * FROM t_skip_empty_compact ORDER BY key;

DROP TABLE t_skip_empty_compact;

-- ============================================================================
-- CASE 12: LowCardinality column — hasOnlyTypeDefaults uses the generic
-- isDefaultAt loop, which works through the dictionary.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_lc;

CREATE TABLE t_skip_empty_lc
(
    key UInt64,
    lc1 LowCardinality(String),
    lc2 LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         skip_empty_columns_on_insert = 1,
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- lc1='' (default), lc2='' (default) → both should be skipped
INSERT INTO t_skip_empty_lc (key, lc1, lc2) VALUES (1, '', '');

SELECT 'case12_columns_in_part';
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_lc' AND active
ORDER BY column;

SELECT 'case12_data';
SELECT * FROM t_skip_empty_lc ORDER BY key;

-- Insert non-default, merge
INSERT INTO t_skip_empty_lc (key, lc1, lc2) VALUES (2, 'hello', 'world');

OPTIMIZE TABLE t_skip_empty_lc FINAL;

SELECT 'case12_post_merge';
SELECT * FROM t_skip_empty_lc ORDER BY key;

DROP TABLE t_skip_empty_lc;

-- ============================================================================
-- CASE 13: Vertical merge — explicitly enable vertical merge algorithm and
-- verify that merging parts with missing columns works correctly.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_vertical;

CREATE TABLE t_skip_empty_vertical
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
         skip_empty_columns_on_insert = 1,
         enable_block_number_column = 0, enable_block_offset_column = 0,
         enable_vertical_merge_algorithm = 1,
         vertical_merge_algorithm_min_rows_to_activate = 1,
         vertical_merge_algorithm_min_columns_to_activate = 1;

INSERT INTO t_skip_empty_vertical (key, a, b, c) VALUES (1, 100, 0, '');
INSERT INTO t_skip_empty_vertical (key, a, b, c) VALUES (2, 0, 200, '');
INSERT INTO t_skip_empty_vertical (key, a, b, c) VALUES (3, 0, 0, 'hello');

OPTIMIZE TABLE t_skip_empty_vertical FINAL;

SELECT 'case13_vertical_merge';
SELECT * FROM t_skip_empty_vertical ORDER BY key;

DROP TABLE t_skip_empty_vertical;

-- ============================================================================
-- CASE 14: Horizontal merge — explicitly disable vertical merge and verify.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_horizontal;

CREATE TABLE t_skip_empty_horizontal
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
         skip_empty_columns_on_insert = 1,
         enable_block_number_column = 0, enable_block_offset_column = 0,
         enable_vertical_merge_algorithm = 0;

INSERT INTO t_skip_empty_horizontal (key, a, b, c) VALUES (1, 100, 0, '');
INSERT INTO t_skip_empty_horizontal (key, a, b, c) VALUES (2, 0, 200, '');
INSERT INTO t_skip_empty_horizontal (key, a, b, c) VALUES (3, 0, 0, 'hello');

OPTIMIZE TABLE t_skip_empty_horizontal FINAL;

SELECT 'case14_horizontal_merge';
SELECT * FROM t_skip_empty_horizontal ORDER BY key;

DROP TABLE t_skip_empty_horizontal;

-- ============================================================================
-- CASE 15: Skipped column retains type-default after ALTER DEFAULT change.
-- When a column is skipped (all values were type-default), changing its DEFAULT
-- expression must NOT affect the read value — serialization.json records the
-- column as skipped, so the reader fills with type-default, not the expression.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_alter_default;

CREATE TABLE t_skip_empty_alter_default
(
    key UInt64,
    a UInt64,
    b UInt64
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         skip_empty_columns_on_insert = 1,
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- b=0 (type-default) → b should be skipped
INSERT INTO t_skip_empty_alter_default (key, a, b) VALUES (1, 100, 0);

SELECT 'case15_pre_alter';
SELECT * FROM t_skip_empty_alter_default ORDER BY key;

-- Change the DEFAULT expression for b. The skipped column must still read as 0.
ALTER TABLE t_skip_empty_alter_default MODIFY COLUMN b UInt64 DEFAULT 999;

SELECT 'case15_post_alter';
SELECT * FROM t_skip_empty_alter_default ORDER BY key;

-- New inserts with b=0 should NOT be skipped (b now has a DEFAULT expression)
INSERT INTO t_skip_empty_alter_default (key, a, b) VALUES (2, 200, 0);

SELECT 'case15_new_insert';
SELECT * FROM t_skip_empty_alter_default ORDER BY key;

DROP TABLE t_skip_empty_alter_default;

-- ============================================================================
-- CASE 16: Enum column whose first declared value is not the zero value.
-- Inserting 'zero' (value 0) produces an all-zero column, but the type-default
-- (IDataType::insertDefaultInto) is the first declared value 'neg' (value -1).
-- Such a column must NOT be skipped, otherwise the read path would fill it with
-- 'neg' and return wrong data.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_enum;

CREATE TABLE t_skip_empty_enum
(
    key UInt64,
    a UInt64,
    e Enum8('neg' = -1, 'zero' = 0, 'pos' = 1)
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         skip_empty_columns_on_insert = 1,
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- e = 'zero' has all-zero bytes but must not be skipped (type-default is 'neg').
INSERT INTO t_skip_empty_enum (key, a, e) VALUES (1, 100, 'zero');

SELECT 'case16_enum';
SELECT key, a, e FROM t_skip_empty_enum ORDER BY key;

DROP TABLE t_skip_empty_enum;
