-- Test: Skip writing all-default columns during MergeTree INSERT.
-- Columns whose values are entirely type-defaults are removed from the part
-- at INSERT time, saving disk space. The data reads back correctly because
-- missing columns are filled with defaults (same as ALTER ADD COLUMN).

-- Disable sparse serialization so it doesn't interfere with our optimization.
-- Disable virtual columns that could add extra columns to the part.
SET mutations_sync = 2;

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
-- CASE 47: Compact marker-only CLEAR rebuilds projections from the cleared value.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case47;

CREATE TABLE t_skip_empty_case47
(
    key UInt64,
    b UInt64,
    PROJECTION p (SELECT key, sum(b) GROUP BY key)
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_case47 VALUES (1, 0);
ALTER TABLE t_skip_empty_case47 MODIFY COLUMN b UInt64 DEFAULT 999;
ALTER TABLE t_skip_empty_case47 CLEAR COLUMN b;

SELECT 'case47_table';
SELECT key, b FROM t_skip_empty_case47 ORDER BY key;

SELECT 'case47_projection';
SELECT key, sum(b) FROM t_skip_empty_case47 GROUP BY key ORDER BY key SETTINGS force_optimize_projection = 1;

DROP TABLE t_skip_empty_case47;

-- ============================================================================
-- CASE 48: Wide marker-only CLEAR rebuilds dependent MATERIALIZED columns.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case48;

CREATE TABLE t_skip_empty_case48
(
    key UInt64,
    b UInt64,
    c UInt64 MATERIALIZED b + 1
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_case48 (key, b) VALUES (1, 0);
ALTER TABLE t_skip_empty_case48 MODIFY COLUMN b UInt64 DEFAULT 999;
ALTER TABLE t_skip_empty_case48 CLEAR COLUMN b;

SELECT 'case48_data';
SELECT key, b, c FROM t_skip_empty_case48 ORDER BY key;

DROP TABLE t_skip_empty_case48;

-- ============================================================================
-- CASE 49: CLEAR only recomputes its readable MATERIALIZED dependency closure.
-- An unrelated MATERIALIZED column backed by EPHEMERAL input cannot be rebuilt
-- from an existing part and must retain its stored value.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case49;

CREATE TABLE t_skip_empty_case49
(
    key UInt64,
    b UInt64,
    e UInt64 EPHEMERAL,
    m1 UInt64 MATERIALIZED b + 1,
    m2 UInt64 MATERIALIZED m1 + 1,
    from_e UInt64 MATERIALIZED e + 1
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_case49 (key, b, e) VALUES (1, 0, 41);
ALTER TABLE t_skip_empty_case49 MODIFY COLUMN b UInt64 DEFAULT 999;
ALTER TABLE t_skip_empty_case49 CLEAR COLUMN b;

SELECT 'case49_data';
SELECT key, b, m1, m2, from_e FROM t_skip_empty_case49 ORDER BY key;

DROP TABLE t_skip_empty_case49;

-- ============================================================================
-- CASE 50: Renamed marker-only type change rebuilds its projection.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case50;

CREATE TABLE t_skip_empty_case50
(
    key UInt64,
    b UInt64
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         index_granularity = 1,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns',
         alter_column_secondary_index_mode = 'rebuild',
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_case50 VALUES (1, 0);
ALTER TABLE t_skip_empty_case50 RENAME COLUMN b TO c;
ALTER TABLE t_skip_empty_case50 ADD PROJECTION p (SELECT c ORDER BY c);
ALTER TABLE t_skip_empty_case50 MATERIALIZE PROJECTION p;
ALTER TABLE t_skip_empty_case50 MODIFY COLUMN c Nullable(UInt64);

SELECT 'case50_projection';
SELECT c FROM t_skip_empty_case50 ORDER BY c SETTINGS force_optimize_projection = 1;

DROP TABLE t_skip_empty_case50;

-- ============================================================================
-- CASE 51: Marker-only CLEAR rebuilds its skip index from the current DEFAULT.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case51;

CREATE TABLE t_skip_empty_case51
(
    key UInt64,
    b UInt64,
    INDEX ix b TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         index_granularity = 1,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns',
         alter_column_secondary_index_mode = 'rebuild',
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_case51 VALUES (1, 0);
ALTER TABLE t_skip_empty_case51 MODIFY COLUMN b UInt64 DEFAULT 999;
ALTER TABLE t_skip_empty_case51 CLEAR COLUMN b;

SELECT 'case51_index_new';
SELECT count() FROM t_skip_empty_case51 WHERE b = 999 SETTINGS force_data_skipping_indices = 'ix';
SELECT 'case51_index_old';
SELECT count() FROM t_skip_empty_case51 WHERE b = 0 SETTINGS force_data_skipping_indices = 'ix';

DROP TABLE t_skip_empty_case51;

-- ============================================================================
-- CASE 52: Renamed physical columns use their part name during type changes.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case52;

CREATE TABLE t_skip_empty_case52
(
    key UInt64,
    b UInt64
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_case52 VALUES (1, 7);
ALTER TABLE t_skip_empty_case52 RENAME COLUMN b TO c;
ALTER TABLE t_skip_empty_case52 MODIFY COLUMN c Nullable(UInt64);

SELECT 'case52_data';
SELECT key, c, toTypeName(c) FROM t_skip_empty_case52 ORDER BY key;

DROP TABLE t_skip_empty_case52;

-- ============================================================================
-- CASE 53: Existing markers survive a later format-version downgrade and merge.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case53;

CREATE TABLE t_skip_empty_case53
(
    key UInt64,
    b UInt64
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_case53 VALUES (1, 0);
INSERT INTO t_skip_empty_case53 VALUES (2, 0);
ALTER TABLE t_skip_empty_case53 MODIFY SETTING serialization_info_version = 'basic';
ALTER TABLE t_skip_empty_case53 MODIFY COLUMN b UInt64 DEFAULT 999;
OPTIMIZE TABLE t_skip_empty_case53 FINAL;
DETACH TABLE t_skip_empty_case53;
ATTACH TABLE t_skip_empty_case53;

SELECT 'case53_merge';
SELECT key, b FROM t_skip_empty_case53 ORDER BY key;

DROP TABLE t_skip_empty_case53;

-- ============================================================================
-- CASE 54: Existing markers survive a downgraded full-rewrite mutation.
-- ============================================================================
DROP TABLE IF EXISTS t_skip_empty_case54;

CREATE TABLE t_skip_empty_case54
(
    key UInt64,
    b UInt64
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_skip_empty_case54 VALUES (1, 0), (2, 0);
ALTER TABLE t_skip_empty_case54 MODIFY SETTING serialization_info_version = 'basic';
ALTER TABLE t_skip_empty_case54 MODIFY COLUMN b UInt64 DEFAULT 999;
ALTER TABLE t_skip_empty_case54 DELETE WHERE key = 1;
DETACH TABLE t_skip_empty_case54;
ATTACH TABLE t_skip_empty_case54;

SELECT 'case54_mutation';
SELECT key, b FROM t_skip_empty_case54 ORDER BY key;

DROP TABLE t_skip_empty_case54;
