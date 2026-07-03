-- Test: Skip writing all-default columns during MergeTree INSERT.
-- Columns whose values are entirely type-defaults are removed from the part
-- at INSERT time, saving disk space. The data reads back correctly because
-- missing columns are filled with defaults (same as ALTER ADD COLUMN).

-- Disable sparse serialization so it doesn't interfere with our optimization.
-- Disable virtual columns that could add extra columns to the part.
SET mutations_sync = 2;

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


-- CASE 46: Date32 type-default is 1900-01-01 (= -25567), NOT memory-zero.
-- Inserting '1970-01-01' (stored as 0 in memory) must NOT be skipped,
-- because the read path fills missing columns with getDefault() = 1900-01-01.

DROP TABLE IF EXISTS t_skip_empty_case46;

CREATE TABLE t_skip_empty_case46
(
    key UInt64,
    d Date32
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- '1970-01-01' is stored as Int32(0) — all-zero bytes in memory.
-- Without the fix, hasOnlyTypeDefaults() sees all-zero → skips → reads back as 1900-01-01. Bug!
INSERT INTO t_skip_empty_case46 VALUES (1, '1970-01-01');

SELECT 'case46_epoch';
SELECT * FROM t_skip_empty_case46 ORDER BY key;

-- The actual type-default 1900-01-01 should also round-trip correctly.
-- Date32 columns are never skipped (type-default ≠ zero), so this is always stored.
INSERT INTO t_skip_empty_case46 VALUES (2, '1900-01-01');

SELECT 'case46_type_default';
SELECT * FROM t_skip_empty_case46 ORDER BY key;

DROP TABLE t_skip_empty_case46;
