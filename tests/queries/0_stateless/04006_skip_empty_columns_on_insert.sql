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
         serialization_info_version = 'with_missing_columns',
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
         serialization_info_version = 'with_missing_columns',
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
         serialization_info_version = 'with_missing_columns',
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
         serialization_info_version = 'with_missing_columns',
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
         serialization_info_version = 'with_missing_columns',
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
         serialization_info_version = 'with_missing_columns',
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
         serialization_info_version = 'with_missing_columns',
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
         serialization_info_version = 'with_missing_columns',
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
         serialization_info_version = 'with_missing_columns',
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
         serialization_info_version = 'with_missing_columns',
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
         serialization_info_version = 'with_missing_columns',
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
         serialization_info_version = 'with_missing_columns',
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
         serialization_info_version = 'with_missing_columns',
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
         serialization_info_version = 'with_missing_columns',
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
-- CASE 15: Missing column retains type-default after ALTER DEFAULT change.
-- When a column is skipped (all values were type-default), changing its DEFAULT
-- expression must NOT affect the read value — serialization.json records the
-- column as missing, so the reader fills with type-default, not the expression.
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- b=0 (type-default) → b should be skipped
INSERT INTO t_skip_empty_alter_default (key, a, b) VALUES (1, 100, 0);

SELECT 'case15_pre_alter';
SELECT * FROM t_skip_empty_alter_default ORDER BY key;

-- Change the DEFAULT expression for b. The missing column must still read as 0.
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- e = 'zero' has all-zero bytes but must not be skipped (type-default is 'neg').
INSERT INTO t_skip_empty_enum (key, a, e) VALUES (1, 100, 'zero');

SELECT 'case16_enum';
SELECT key, a, e FROM t_skip_empty_enum ORDER BY key;

DROP TABLE t_skip_empty_enum;

-- ============================================================================
-- CASE 17: A missing column keeps its inserted type-default after an unrelated
-- mutation, even once it later gains a DEFAULT expression. The missing-columns
-- marker must be propagated through mutations; otherwise it is dropped and the
-- read path evaluates the new DEFAULT expression instead of the inserted value.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_mutate_default;

CREATE TABLE t_skip_empty_mutate_default
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- b=0 (type-default) → b is skipped.
INSERT INTO t_skip_empty_mutate_default (key, a, b) VALUES (1, 100, 0);

SELECT 'case17_columns_in_part';
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_mutate_default' AND active
ORDER BY column;

-- Give b a DEFAULT expression (metadata-only), then mutate an unrelated column.
ALTER TABLE t_skip_empty_mutate_default MODIFY COLUMN b UInt64 DEFAULT 999;
ALTER TABLE t_skip_empty_mutate_default UPDATE a = a + 1 WHERE key = 1;

-- b must still read as 0 (the inserted type-default), not 999.
SELECT 'case17_post_mutate';
SELECT * FROM t_skip_empty_mutate_default ORDER BY key;

DROP TABLE t_skip_empty_mutate_default;

-- ============================================================================
-- CASE 18: A column missing in every source part keeps its inserted type-default
-- after a merge followed by a DEFAULT change. The missing-columns marker must be
-- propagated through the merge; otherwise the merged part is written without it
-- and the read path evaluates the new DEFAULT expression.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_merge_default;

CREATE TABLE t_skip_empty_merge_default
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- b=0 (type-default) in both parts → b is skipped in both.
INSERT INTO t_skip_empty_merge_default (key, a, b) VALUES (1, 100, 0);
INSERT INTO t_skip_empty_merge_default (key, a, b) VALUES (2, 200, 0);

OPTIMIZE TABLE t_skip_empty_merge_default FINAL;

SELECT 'case18_columns_in_part';
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_merge_default' AND active
ORDER BY column;

ALTER TABLE t_skip_empty_merge_default MODIFY COLUMN b UInt64 DEFAULT 999;

-- b must still read as 0 (the inserted type-default), not 999.
SELECT 'case18_post_merge_alter';
SELECT * FROM t_skip_empty_merge_default ORDER BY key;

DROP TABLE t_skip_empty_merge_default;

-- ============================================================================
-- CASE 19: A missing column that is renamed on the fly keeps its inserted
-- type-default after a DEFAULT change on the new name. fillMissingColumns must
-- translate the requested name back through alter_conversions before consulting
-- the missing-columns marker (recorded under the original physical name).
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_rename_default;

CREATE TABLE t_skip_empty_rename_default
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- b=0 (type-default) → b is skipped.
INSERT INTO t_skip_empty_rename_default (key, a, b) VALUES (1, 100, 0);

-- Rename the missing column, then give the new name a DEFAULT expression.
ALTER TABLE t_skip_empty_rename_default RENAME COLUMN b TO c;
ALTER TABLE t_skip_empty_rename_default MODIFY COLUMN c UInt64 DEFAULT 999;

-- c must still read as 0 (the inserted type-default), not 999.
SELECT 'case19_post_rename_alter';
SELECT key, a, c FROM t_skip_empty_rename_default ORDER BY key;

DROP TABLE t_skip_empty_rename_default;

-- ============================================================================
-- CASE 20: The optimization requires serialization_info_version >=
-- 'with_missing_columns'. When a lower version is configured (here 'with_types',
-- e.g. pinned for a rolling upgrade so older servers can read new parts), no
-- columns are skipped even with skip_empty_columns_on_insert enabled.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_version_gate;

CREATE TABLE t_skip_empty_version_gate
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
         serialization_info_version = 'with_types',
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- b='' (type-default) but version < with_missing_columns → b is NOT skipped.
INSERT INTO t_skip_empty_version_gate (key, a, b) VALUES (1, 100, '');

SELECT 'case20_columns_in_part';
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_version_gate' AND active
ORDER BY column;

SELECT 'case20_data';
SELECT * FROM t_skip_empty_version_gate ORDER BY key;

DROP TABLE t_skip_empty_version_gate;

-- ============================================================================
-- CASE 21: A missing column that is renamed on the fly and then merged keeps its
-- inserted type-default after a DEFAULT change on the new name. The merge
-- materializes the rename into the merged part (written at the current metadata
-- version, so the on-the-fly rename conversion no longer exists on read), so
-- MergeTask must translate each source part's missing-columns marker through its
-- rename map before storing it on the merged part. Otherwise the merged part
-- records the stale pre-rename name, fillMissingColumns misses it, and the later
-- ALTER MODIFY COLUMN ... DEFAULT returns the new default instead of the inserted
-- type-default. This is the merge-after-rename order, distinct from rename only
-- (case 19) and merge only (case 18).
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_rename_merge_default;

CREATE TABLE t_skip_empty_rename_merge_default
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- b=0 (type-default) in both parts → b is skipped in both.
INSERT INTO t_skip_empty_rename_merge_default (key, a, b) VALUES (1, 100, 0);
INSERT INTO t_skip_empty_rename_merge_default (key, a, b) VALUES (2, 200, 0);

-- Rename the missing column, then merge: the merged part is written under the
-- new name c and must carry the missing-columns marker as c (not the stale b).
ALTER TABLE t_skip_empty_rename_merge_default RENAME COLUMN b TO c;
OPTIMIZE TABLE t_skip_empty_rename_merge_default FINAL;

SELECT 'case21_columns_in_part';
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_rename_merge_default' AND active
ORDER BY column;

-- Give the new name a DEFAULT expression. c must still read as 0, not 999.
ALTER TABLE t_skip_empty_rename_merge_default MODIFY COLUMN c UInt64 DEFAULT 999;

SELECT 'case21_post_rename_merge_alter';
SELECT key, a, c FROM t_skip_empty_rename_merge_default ORDER BY key;

DROP TABLE t_skip_empty_rename_merge_default;

-- ============================================================================
-- CASE 22: DETACH TABLE / ATTACH TABLE preserves the missing_columns marker.
-- Re-reading metadata from disk must restore the missing-columns marker.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case22;

CREATE TABLE t_skip_empty_case22
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_case22 (key, a, b) VALUES (1, 100, 0);

SELECT 'case22_pre_detach';
SELECT * FROM t_skip_empty_case22 ORDER BY key;

-- Add a DEFAULT expression, then detach/attach to force metadata re-read from disk.
ALTER TABLE t_skip_empty_case22 MODIFY COLUMN b UInt64 DEFAULT 999;
DETACH TABLE t_skip_empty_case22;
ATTACH TABLE t_skip_empty_case22;

-- b must still read as 0 (marker preserved on disk), not 999.
SELECT 'case22_post_attach';
SELECT * FROM t_skip_empty_case22 ORDER BY key;

DROP TABLE t_skip_empty_case22;

-- ============================================================================
-- CASE 23: DETACH PARTITION / ATTACH PARTITION preserves the marker.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case23;

CREATE TABLE t_skip_empty_case23
(
    key UInt64,
    a UInt64,
    b UInt64
)
ENGINE = MergeTree
PARTITION BY key
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_case23 (key, a, b) VALUES (1, 100, 0);

SELECT 'case23_pre_detach';
SELECT * FROM t_skip_empty_case23 ORDER BY key;

ALTER TABLE t_skip_empty_case23 DETACH PARTITION 1;
ALTER TABLE t_skip_empty_case23 MODIFY COLUMN b UInt64 DEFAULT 999;
ALTER TABLE t_skip_empty_case23 ATTACH PARTITION 1;

-- b must still read as 0 (marker preserved through detach/attach), not 999.
SELECT 'case23_post_attach';
SELECT * FROM t_skip_empty_case23 ORDER BY key;

DROP TABLE t_skip_empty_case23;

-- ============================================================================
-- CASE 24: BACKUP and RESTORE preserves the missing_columns marker.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case24;

CREATE TABLE t_skip_empty_case24
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_case24 (key, a, b) VALUES (1, 100, 0);

SELECT 'case24_pre_backup';
SELECT * FROM t_skip_empty_case24 ORDER BY key;

BACKUP TABLE t_skip_empty_case24 TO Memory('04006_case24') FORMAT Null;
DROP TABLE t_skip_empty_case24 SYNC;
RESTORE TABLE t_skip_empty_case24 FROM Memory('04006_case24') FORMAT Null;

-- After restore, add a DEFAULT expression. The marker must survive the backup cycle.
ALTER TABLE t_skip_empty_case24 MODIFY COLUMN b UInt64 DEFAULT 999;

SELECT 'case24_post_restore';
SELECT * FROM t_skip_empty_case24 ORDER BY key;

DROP TABLE t_skip_empty_case24;

-- ============================================================================
-- CASE 25: ALTER TABLE FREEZE does not crash on parts with missing columns.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case25;

CREATE TABLE t_skip_empty_case25
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_case25 (key, a, b) VALUES (1, 100, 0);

ALTER TABLE t_skip_empty_case25 FREEZE;

SELECT 'case25_post_freeze';
SELECT * FROM t_skip_empty_case25 ORDER BY key;

DROP TABLE t_skip_empty_case25;

-- ============================================================================
-- CASE 26: CHECK TABLE passes for parts with missing columns.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case26;

CREATE TABLE t_skip_empty_case26
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_case26 (key, a, b) VALUES (1, 100, 0);

SELECT 'case26_check_table';
CHECK TABLE t_skip_empty_case26 SETTINGS check_query_single_value_result = 1;

DROP TABLE t_skip_empty_case26;

-- ============================================================================
-- CASE 27: MATERIALIZE COLUMN on a missing column.
-- After materialization, the DEFAULT expression is evaluated and written.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case27;

CREATE TABLE t_skip_empty_case27
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_case27 (key, a, b) VALUES (1, 100, 0);

SELECT 'case27_pre_materialize';
SELECT * FROM t_skip_empty_case27 ORDER BY key;

ALTER TABLE t_skip_empty_case27 MODIFY COLUMN b UInt64 DEFAULT 999;
ALTER TABLE t_skip_empty_case27 MATERIALIZE COLUMN b;

-- After materialization, b should be 999 (DEFAULT evaluated and written).
SELECT 'case27_post_materialize';
SELECT * FROM t_skip_empty_case27 ORDER BY key;

DROP TABLE t_skip_empty_case27;

-- ============================================================================
-- CASE 28: CLEAR COLUMN on a missing column.
-- CLEAR is effectively a no-op since the column has no physical data.
-- The missing_columns marker remains; read still returns the frozen type-default.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case28;

CREATE TABLE t_skip_empty_case28
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_case28 (key, a, b) VALUES (1, 100, 0);

SELECT 'case28_pre_clear';
SELECT * FROM t_skip_empty_case28 ORDER BY key;

ALTER TABLE t_skip_empty_case28 MODIFY COLUMN b UInt64 DEFAULT 999;
ALTER TABLE t_skip_empty_case28 CLEAR COLUMN b;

-- After clear, b still has no data and marker remains → frozen type-default = 0.
SELECT 'case28_post_clear';
SELECT * FROM t_skip_empty_case28 ORDER BY key;

DROP TABLE t_skip_empty_case28;

-- ============================================================================
-- CASE 29: Lightweight DELETE with missing columns.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case29;

CREATE TABLE t_skip_empty_case29
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_case29 (key, a, b) VALUES (1, 100, 0), (2, 200, 0);

SELECT 'case29_pre_delete';
SELECT * FROM t_skip_empty_case29 ORDER BY key;

DELETE FROM t_skip_empty_case29 WHERE key = 1;

-- Remaining rows must still have b = 0.
SELECT 'case29_post_delete';
SELECT * FROM t_skip_empty_case29 ORDER BY key;

DROP TABLE t_skip_empty_case29;

-- ============================================================================
-- CASE 30: Multiple mutations in sequence preserve the missing-columns marker.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case30;

CREATE TABLE t_skip_empty_case30
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_case30 (key, a, b) VALUES (1, 100, 0);

ALTER TABLE t_skip_empty_case30 MODIFY COLUMN b UInt64 DEFAULT 999;
ALTER TABLE t_skip_empty_case30 UPDATE a = a + 1 WHERE 1;
ALTER TABLE t_skip_empty_case30 UPDATE a = a + 1 WHERE 1;

-- b must still read as 0 after chained unrelated mutations.
SELECT 'case30_post_mutations';
SELECT * FROM t_skip_empty_case30 ORDER BY key;

DROP TABLE t_skip_empty_case30;

-- ============================================================================
-- CASE 31: INSERT SELECT from a table with missing columns to another table.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case31_src;
DROP TABLE IF EXISTS t_skip_empty_case31_dst;

CREATE TABLE t_skip_empty_case31_src
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

CREATE TABLE t_skip_empty_case31_dst
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- b=0 (missing in src)
INSERT INTO t_skip_empty_case31_src (key, a, b) VALUES (1, 100, 0);

-- INSERT SELECT: b=0 flows through and should be skipped in dst too.
INSERT INTO t_skip_empty_case31_dst SELECT * FROM t_skip_empty_case31_src;

SELECT 'case31_columns_in_dst';
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_case31_dst' AND active
ORDER BY column;

SELECT 'case31_data';
SELECT * FROM t_skip_empty_case31_dst ORDER BY key;

DROP TABLE t_skip_empty_case31_src;
DROP TABLE t_skip_empty_case31_dst;

-- ============================================================================
-- CASE 32: REPLACE PARTITION from a table with missing columns.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case32_src;
DROP TABLE IF EXISTS t_skip_empty_case32_dst;

CREATE TABLE t_skip_empty_case32_src
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

CREATE TABLE t_skip_empty_case32_dst
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_case32_src (key, a, b) VALUES (1, 100, 0);

ALTER TABLE t_skip_empty_case32_dst REPLACE PARTITION ID 'all' FROM t_skip_empty_case32_src;

SELECT 'case32_data';
SELECT * FROM t_skip_empty_case32_dst ORDER BY key;

-- The marker must survive the partition move.
ALTER TABLE t_skip_empty_case32_dst MODIFY COLUMN b UInt64 DEFAULT 999;

SELECT 'case32_post_alter';
SELECT * FROM t_skip_empty_case32_dst ORDER BY key;

DROP TABLE t_skip_empty_case32_src;
DROP TABLE t_skip_empty_case32_dst;

-- ============================================================================
-- CASE 33: ATTACH PARTITION FROM (move partition between tables).
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case33_src;
DROP TABLE IF EXISTS t_skip_empty_case33_dst;

CREATE TABLE t_skip_empty_case33_src
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

CREATE TABLE t_skip_empty_case33_dst
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_case33_src (key, a, b) VALUES (1, 100, 0);

ALTER TABLE t_skip_empty_case33_dst ATTACH PARTITION ID 'all' FROM t_skip_empty_case33_src;

SELECT 'case33_data';
SELECT * FROM t_skip_empty_case33_dst ORDER BY key;

ALTER TABLE t_skip_empty_case33_dst MODIFY COLUMN b UInt64 DEFAULT 999;

SELECT 'case33_post_alter';
SELECT * FROM t_skip_empty_case33_dst ORDER BY key;

DROP TABLE t_skip_empty_case33_src;
DROP TABLE t_skip_empty_case33_dst;

-- ============================================================================
-- CASE 34: ALTER ADD COLUMN interaction with existing missing columns.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case34;

CREATE TABLE t_skip_empty_case34
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- b=0 (missing)
INSERT INTO t_skip_empty_case34 (key, a, b) VALUES (1, 100, 0);

-- Add a new column with a DEFAULT expression.
ALTER TABLE t_skip_empty_case34 ADD COLUMN c UInt64 DEFAULT 42;

-- b=0 (from marker), c=42 (from DEFAULT expr — no marker for c, it was added after the part).
SELECT 'case34_data';
SELECT key, a, b, c FROM t_skip_empty_case34 ORDER BY key;

DROP TABLE t_skip_empty_case34;

-- ============================================================================
-- CASE 35: ALTER DROP COLUMN that was missing.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case35;

CREATE TABLE t_skip_empty_case35
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_case35 (key, a, b) VALUES (1, 100, 0);

ALTER TABLE t_skip_empty_case35 DROP COLUMN b;

-- Table should work fine with b gone.
SELECT 'case35_post_drop';
SELECT * FROM t_skip_empty_case35 ORDER BY key;

DROP TABLE t_skip_empty_case35;

-- ============================================================================
-- CASE 36: Projections with missing columns.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case36;

CREATE TABLE t_skip_empty_case36
(
    key UInt64,
    a UInt64,
    b UInt64,
    PROJECTION p1 (SELECT key, sum(a), sum(b) GROUP BY key)
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_case36 (key, a, b) VALUES (1, 100, 0);

SELECT 'case36_data';
SELECT * FROM t_skip_empty_case36 ORDER BY key;

SELECT 'case36_projection';
SELECT key, sum(a), sum(b) FROM t_skip_empty_case36 GROUP BY key ORDER BY key;

DROP TABLE t_skip_empty_case36;

-- ============================================================================
-- CASE 37: FixedString column — all-zero bytes is the type-default.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case37;

CREATE TABLE t_skip_empty_case37
(
    key UInt64,
    fs FixedString(4)
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- FixedString(4) type-default is 4 zero bytes; empty string is zero-padded → should be skipped.
INSERT INTO t_skip_empty_case37 (key, fs) VALUES (1, '');

SELECT 'case37_columns_in_part';
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_case37' AND active
ORDER BY column;

SELECT 'case37_data';
SELECT key, hex(fs) FROM t_skip_empty_case37 ORDER BY key;

DROP TABLE t_skip_empty_case37;

-- ============================================================================
-- CASE 38: Map column — empty map is the type-default.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case38;

CREATE TABLE t_skip_empty_case38
(
    key UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- Empty map is the type-default → should be skipped.
INSERT INTO t_skip_empty_case38 (key, m) VALUES (1, map());

SELECT 'case38_columns_in_part';
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_case38' AND active
ORDER BY column;

SELECT 'case38_data';
SELECT * FROM t_skip_empty_case38 ORDER BY key;

DROP TABLE t_skip_empty_case38;

-- ============================================================================
-- CASE 39: Multiple INSERTs — some with missing cols, some without — then merge.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case39;

CREATE TABLE t_skip_empty_case39
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- Part 1: b=0 (missing)
INSERT INTO t_skip_empty_case39 (key, a, b) VALUES (1, 10, 0);
-- Part 2: b=100 (NOT skipped)
INSERT INTO t_skip_empty_case39 (key, a, b) VALUES (2, 20, 100);
-- Part 3: b=0 (missing)
INSERT INTO t_skip_empty_case39 (key, a, b) VALUES (3, 30, 0);

SELECT 'case39_pre_merge';
SELECT * FROM t_skip_empty_case39 ORDER BY key;

OPTIMIZE TABLE t_skip_empty_case39 FINAL;

SELECT 'case39_post_merge';
SELECT * FROM t_skip_empty_case39 ORDER BY key;

DROP TABLE t_skip_empty_case39;

-- ============================================================================
-- CASE 40: Date and DateTime columns — epoch zero is the type-default.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case40;

CREATE TABLE t_skip_empty_case40
(
    key UInt64,
    d Date,
    dt DateTime('UTC')
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- Date=0 ('1970-01-01'), DateTime=0 ('1970-01-01 00:00:00') → should be skipped.
INSERT INTO t_skip_empty_case40 (key, d, dt) VALUES (1, '1970-01-01', '1970-01-01 00:00:00');

SELECT 'case40_columns_in_part';
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_case40' AND active
ORDER BY column;

SELECT 'case40_data';
SELECT * FROM t_skip_empty_case40 ORDER BY key;

DROP TABLE t_skip_empty_case40;

-- ============================================================================
-- CASE 41: Merge of pre-ADD-COLUMN part (no marker) + post-skip part (with marker).
-- Regression for AI reviewer blocker #1: when one source part predates
-- ALTER ADD COLUMN (missing column, no marker) and another source part has the
-- column skipped via skip_empty_columns_on_insert (missing column, with marker),
-- the merge must produce correct per-row values:
--   - pre-ADD-COLUMN row → current DEFAULT (999)
--   - skipped row → frozen type-default (0)
-- The merged part materializes the column (because b is in storage_columns),
-- so the marker is NOT propagated — each row gets its correct value baked in.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case41;

-- Step 1: Create table WITHOUT column b, insert a row.
CREATE TABLE t_skip_empty_case41
(
    key UInt64,
    a UInt64
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_case41 (key, a) VALUES (1, 100);

-- Step 2: ADD COLUMN b with a DEFAULT, then enable skip and insert b=0.
ALTER TABLE t_skip_empty_case41 ADD COLUMN b UInt64 DEFAULT 999;
ALTER TABLE t_skip_empty_case41 MODIFY SETTING skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';

INSERT INTO t_skip_empty_case41 (key, a, b) VALUES (2, 200, 0);

-- Pre-merge: row 1 should read b=999 (no marker, current DEFAULT),
-- row 2 should read b=0 (marker, frozen type-default).
SELECT 'case41_pre_merge';
SELECT * FROM t_skip_empty_case41 ORDER BY key;

-- Merge: both parts lack b physically, but part1 has no marker, part2 has marker.
-- After merge, both values are materialized into the merged part.
OPTIMIZE TABLE t_skip_empty_case41 FINAL;

SELECT 'case41_post_merge';
SELECT * FROM t_skip_empty_case41 ORDER BY key;

-- Change DEFAULT again — merged part has physical data, so values don't change.
ALTER TABLE t_skip_empty_case41 MODIFY COLUMN b UInt64 DEFAULT 777;

SELECT 'case41_post_alter';
SELECT * FROM t_skip_empty_case41 ORDER BY key;

DROP TABLE t_skip_empty_case41;

-- ============================================================================
-- CASE 42: Type-changing ALTER MODIFY COLUMN on a missing column.
-- Regression for AI reviewer blocker #3: when a column was skipped (b UInt64 = 0),
-- then ALTER MODIFY COLUMN b Nullable(UInt64), the missing column is filled with
-- the NEW type's type-default (NULL for Nullable). This is by design: the marker
-- records "fill with type-default" which is type-specific, and a type change via
-- mutation materializes the column anyway.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case42;

CREATE TABLE t_skip_empty_case42
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_case42 (key, a, b) VALUES (1, 100, 0);

SELECT 'case42_pre_modify';
SELECT * FROM t_skip_empty_case42 ORDER BY key;

-- Type-changing mutation: UInt64 → Nullable(UInt64).
-- The mutation materializes the column, so the marker is removed.
-- MATERIALIZE happens via updated_header containing 'b'.
ALTER TABLE t_skip_empty_case42 MODIFY COLUMN b Nullable(UInt64);

SELECT 'case42_post_modify';
SELECT * FROM t_skip_empty_case42 ORDER BY key;

DROP TABLE t_skip_empty_case42;

-- ============================================================================
-- CASE 43: Compact part rename of a missing column followed by mutation.
-- Regression for AI reviewer blocker #4: a missing column on a compact part is
-- renamed, then an unrelated mutation fires. The rename must be tracked in
-- serialization_infos so the marker survives under the new name.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case43;

CREATE TABLE t_skip_empty_case43
(
    key UInt64,
    a UInt64,
    b UInt64
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- Compact part, b=0 → missing.
INSERT INTO t_skip_empty_case43 (key, a, b) VALUES (1, 100, 0);

-- Rename missing column b → c.
ALTER TABLE t_skip_empty_case43 RENAME COLUMN b TO c;

-- Unrelated mutation — must propagate marker under new name 'c'.
ALTER TABLE t_skip_empty_case43 UPDATE a = a + 1 WHERE key = 1;

-- Add DEFAULT on renamed column. Marker must still shield.
ALTER TABLE t_skip_empty_case43 MODIFY COLUMN c UInt64 DEFAULT 999;

SELECT 'case43_compact_rename_mutate';
SELECT key, a, c FROM t_skip_empty_case43 ORDER BY key;

DROP TABLE t_skip_empty_case43;

-- ============================================================================
-- CASE 44: MATERIALIZE COLUMN on a missing column writes current DEFAULT.
-- Regression for AI reviewer blocker #5: explicitly proving that MATERIALIZE
-- COLUMN computes the current DEFAULT and removes the marker, which is the
-- intended semantic (MATERIALIZE = "write physical data for this column").
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case44;

CREATE TABLE t_skip_empty_case44
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_case44 (key, a, b) VALUES (1, 100, 0);

-- b is missing, reads as 0.
SELECT 'case44_pre';
SELECT * FROM t_skip_empty_case44 ORDER BY key;

-- Add DEFAULT, then MATERIALIZE. After materialize, b is physical data = 999.
ALTER TABLE t_skip_empty_case44 MODIFY COLUMN b UInt64 DEFAULT 999;
ALTER TABLE t_skip_empty_case44 MATERIALIZE COLUMN b;

SELECT 'case44_post_materialize';
SELECT * FROM t_skip_empty_case44 ORDER BY key;

-- Verify: column b is now in the part (materialized, marker gone).
SELECT 'case44_columns_in_part';
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_case44' AND active
ORDER BY column;

-- Change DEFAULT again. Since b is physical data, value stays 999.
ALTER TABLE t_skip_empty_case44 MODIFY COLUMN b UInt64 DEFAULT 777;

SELECT 'case44_post_second_alter';
SELECT * FROM t_skip_empty_case44 ORDER BY key;

DROP TABLE t_skip_empty_case44;

-- ============================================================================
-- CASE 45: CLEAR COLUMN on a missing column is a no-op (marker preserved).
-- Regression for AI reviewer blocker #6: CLEAR COLUMN has nothing to clear
-- when the column has no physical data. The marker remains, and the frozen
-- type-default continues to be returned.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case45;

CREATE TABLE t_skip_empty_case45
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
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_case45 (key, a, b) VALUES (1, 100, 0);

-- Add DEFAULT, then CLEAR. Since b has no physical data, CLEAR is no-op.
ALTER TABLE t_skip_empty_case45 MODIFY COLUMN b UInt64 DEFAULT 999;
ALTER TABLE t_skip_empty_case45 CLEAR COLUMN b;

-- Marker survives → still reads frozen type-default 0, NOT current DEFAULT 999.
SELECT 'case45_post_clear';
SELECT * FROM t_skip_empty_case45 ORDER BY key;

-- Unrelated mutation — marker must still survive.
ALTER TABLE t_skip_empty_case45 UPDATE a = a + 1 WHERE key = 1;

SELECT 'case45_post_mutate';
SELECT * FROM t_skip_empty_case45 ORDER BY key;

DROP TABLE t_skip_empty_case45;
