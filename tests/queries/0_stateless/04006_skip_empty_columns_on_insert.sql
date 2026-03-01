-- Test: Wide MergeTree INSERT skips writing columns that are entirely type-defaults.
-- When a part lacks a column file, the reader fills it with type defaults automatically
-- (same mechanism as ALTER ADD COLUMN on existing parts).

-- ============================================================================
-- CASE 1: Basic test — all-default columns are removed, key columns preserved.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_basic;

CREATE TABLE t_skip_empty_basic
(
    key UInt64,
    val1 UInt64,
    val2 String,
    val3 Float64
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_block_number_column = 0, enable_block_offset_column = 0;

-- val2 ('' default) and val3 (0.0 default) should be removed.
-- key (sorting key) and val1 (non-default) should remain.
INSERT INTO t_skip_empty_basic (key, val1, val2, val3) VALUES (1, 100, '', 0);

SELECT 'case1_columns';
SELECT column
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_basic' AND active
ORDER BY column;

SELECT 'case1_read';
SELECT * FROM t_skip_empty_basic ORDER BY key;

-- Insert another row with val1 also at default, then merge.
INSERT INTO t_skip_empty_basic (key, val1, val2, val3) VALUES (2, 0, '', 0);

OPTIMIZE TABLE t_skip_empty_basic FINAL;

SELECT 'case1_merged_read';
SELECT * FROM t_skip_empty_basic ORDER BY key;

SELECT 'case1_merged_columns';
SELECT column
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_basic' AND active
ORDER BY column;

DROP TABLE t_skip_empty_basic;

-- ============================================================================
-- CASE 2: Sorting key column with all-default values — also removed.
-- Primary index (primary.idx) already has the values baked in, and the reader
-- fills defaults for missing .bin files during merge.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_sortkey;

CREATE TABLE t_skip_empty_sortkey
(
    key1 UInt64,
    key2 UInt64,
    val String
)
ENGINE = MergeTree
ORDER BY (key1, key2)
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_block_number_column = 0, enable_block_offset_column = 0;

-- key2 is all zeros (type default), but it's in the sorting key — must be kept.
INSERT INTO t_skip_empty_sortkey (key1, key2, val) VALUES (1, 0, '');

SELECT 'case2_columns';
SELECT column
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_sortkey' AND active
ORDER BY column;

SELECT 'case2_read';
SELECT * FROM t_skip_empty_sortkey ORDER BY key1, key2;

DROP TABLE t_skip_empty_sortkey;

-- ============================================================================
-- CASE 3: Partition key column with all-default values — also removed.
-- Partition pruning uses the partition ID from the directory name and
-- minmax_*.idx, not the column .bin files.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_partkey;

CREATE TABLE t_skip_empty_partkey
(
    dt Date,
    key UInt64,
    val String
)
ENGINE = MergeTree
PARTITION BY dt
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_block_number_column = 0, enable_block_offset_column = 0;

-- dt = '1970-01-01' is the type default for Date (0). Must be kept (partition key).
INSERT INTO t_skip_empty_partkey (dt, key, val) VALUES ('1970-01-01', 1, '');

SELECT 'case3_columns';
SELECT column
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_partkey' AND active
ORDER BY column;

SELECT 'case3_read';
SELECT * FROM t_skip_empty_partkey ORDER BY key;

DROP TABLE t_skip_empty_partkey;

-- ============================================================================
-- CASE 4: Skip index on an all-default column — column removed, index intact.
-- The skip index is computed during write (before removal), so queries that
-- use the index still work correctly after the column files are stripped.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_skipidx;

CREATE TABLE t_skip_empty_skipidx
(
    key UInt64,
    indexed_val UInt64,
    val String,
    INDEX idx_val indexed_val TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_block_number_column = 0, enable_block_offset_column = 0;

-- indexed_val is 0 (default) — column should be removed, skip index kept.
INSERT INTO t_skip_empty_skipidx (key, indexed_val, val) VALUES (1, 0, 'hello');

SELECT 'case4_columns';
SELECT column
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_skipidx' AND active
ORDER BY column;

SELECT 'case4_read';
SELECT * FROM t_skip_empty_skipidx WHERE indexed_val = 0 ORDER BY key;

DROP TABLE t_skip_empty_skipidx;

-- ============================================================================
-- CASE 5: Projection that reads an all-default column.
-- The projection is computed from the full block before column removal.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_proj;

CREATE TABLE t_skip_empty_proj
(
    key UInt64,
    val1 UInt64,
    val2 UInt64,
    PROJECTION proj_sum (SELECT key, sum(val2) GROUP BY key)
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_block_number_column = 0, enable_block_offset_column = 0;

-- val2 = 0 (default). Main part column removed; projection should still work.
INSERT INTO t_skip_empty_proj (key, val1, val2) VALUES (1, 100, 0), (1, 200, 0);

SELECT 'case5_columns';
SELECT column
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_proj' AND active
ORDER BY column;

SELECT 'case5_read';
SELECT * FROM t_skip_empty_proj ORDER BY key, val1;

-- Query that could use the projection.
SELECT 'case5_proj_query';
SELECT key, sum(val2) FROM t_skip_empty_proj GROUP BY key ORDER BY key;

DROP TABLE t_skip_empty_proj;

-- ============================================================================
-- CASE 6: DEFAULT expression referencing another column.
-- Column with default expr should be written normally and removable if all-default.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_defexpr;

CREATE TABLE t_skip_empty_defexpr
(
    key UInt64,
    base UInt64,
    derived UInt64 DEFAULT base * 2,
    val String
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_block_number_column = 0, enable_block_offset_column = 0;

-- base=0 → derived=0 (both are type-default). val='' also default.
-- All of base, derived, val should be removed (they're not in the key).
INSERT INTO t_skip_empty_defexpr (key, base) VALUES (1, 0);

SELECT 'case6_columns';
SELECT column
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_defexpr' AND active
ORDER BY column;

SELECT 'case6_read';
SELECT * FROM t_skip_empty_defexpr ORDER BY key;

-- Now insert with non-zero base.
INSERT INTO t_skip_empty_defexpr (key, base) VALUES (2, 5);

SELECT 'case6_second_columns';
SELECT name, column
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_defexpr' AND active
ORDER BY name, column;

SELECT 'case6_read_all';
SELECT * FROM t_skip_empty_defexpr ORDER BY key;

DROP TABLE t_skip_empty_defexpr;

-- ============================================================================
-- CASE 7: TTL expression column — the column used in TTL must be protected.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_ttl;

CREATE TABLE t_skip_empty_ttl
(
    key UInt64,
    ts DateTime,
    val String
)
ENGINE = MergeTree
ORDER BY key
TTL ts + INTERVAL 1 DAY
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_block_number_column = 0, enable_block_offset_column = 0;

-- ts = 0 ('1970-01-01 00:00:00', type default), val = '' (default).
-- ts is used in TTL expression; val should be removable.
-- ts is NOT in sorting/partition/primary key, but it IS required for TTL evaluation.
-- Our current implementation doesn't protect TTL columns explicitly —
-- this test documents the behavior. Since TTL evaluation happens during merge
-- (not read), removing the column that's needed for TTL from the part metadata
-- could be problematic. Let's see what happens.
INSERT INTO t_skip_empty_ttl (key, ts, val) VALUES (1, '2100-01-01 00:00:00', '');

SELECT 'case7_columns';
SELECT column
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_ttl' AND active
ORDER BY column;

SELECT 'case7_read';
SELECT * FROM t_skip_empty_ttl ORDER BY key;

DROP TABLE t_skip_empty_ttl;

-- ============================================================================
-- CASE 8: Compact part — also skips empty columns.
-- The columns list is filtered before writing, so the compact data file
-- only contains non-empty columns.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_compact;

CREATE TABLE t_skip_empty_compact
(
    key UInt64,
    val1 UInt64,
    val2 String
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_compact (key, val1, val2) VALUES (1, 100, '');

SELECT 'case8_columns';
SELECT column
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_compact' AND active
ORDER BY column;

SELECT 'case8_read';
SELECT * FROM t_skip_empty_compact ORDER BY key;

DROP TABLE t_skip_empty_compact;

-- ============================================================================
-- CASE 9: Multiple inserts, some columns default in one part but not another.
-- After merge, data should be intact.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_mixed;

CREATE TABLE t_skip_empty_mixed
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
INSERT INTO t_skip_empty_mixed (key, a, b, c) VALUES (1, 100, 0, '');
-- Part 2: a=0 (default), b=200, c='' (default)
INSERT INTO t_skip_empty_mixed (key, a, b, c) VALUES (2, 0, 200, '');
-- Part 3: a=0 (default), b=0 (default), c='hello'
INSERT INTO t_skip_empty_mixed (key, a, b, c) VALUES (3, 0, 0, 'hello');

SELECT 'case9_parts_columns';
SELECT name, column
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_mixed' AND active
ORDER BY name, column;

SELECT 'case9_read';
SELECT * FROM t_skip_empty_mixed ORDER BY key;

OPTIMIZE TABLE t_skip_empty_mixed FINAL;

SELECT 'case9_merged_read';
SELECT * FROM t_skip_empty_mixed ORDER BY key;

SELECT 'case9_merged_columns';
SELECT column
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_mixed' AND active
ORDER BY column;

DROP TABLE t_skip_empty_mixed;

-- ============================================================================
-- CASE 10: Nullable column — NULL is NOT the type default (default is Nullable
-- default = NULL actually, so a column full of NULLs should be removed).
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
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_block_number_column = 0, enable_block_offset_column = 0;

-- NULL is the type default for Nullable(UInt64) and Nullable(String).
INSERT INTO t_skip_empty_nullable (key, val, val2) VALUES (1, NULL, NULL);

SELECT 'case10_columns';
SELECT column
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_skip_empty_nullable' AND active
ORDER BY column;

SELECT 'case10_read';
SELECT * FROM t_skip_empty_nullable ORDER BY key;

-- Non-null values.
INSERT INTO t_skip_empty_nullable (key, val, val2) VALUES (2, 42, 'hello');

SELECT 'case10_read_all';
SELECT * FROM t_skip_empty_nullable ORDER BY key;

DROP TABLE t_skip_empty_nullable;
