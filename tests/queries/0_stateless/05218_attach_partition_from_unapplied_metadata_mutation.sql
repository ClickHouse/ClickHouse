-- Tags: zookeeper, no-replicated-database
-- zookeeper: most sources below are `ReplicatedMergeTree`, which needs Keeper.
-- no-replicated-database: the oracles below count parts and sum columns per table, which the extra
-- shard perturbs (same reason as 03100_lwu_51_replace_partition_pending_patch on this code path).

-- A part whose data is at an older schema version than its own table reads correctly in that table:
-- the pending RENAME COLUMN / DROP COLUMN is applied on the fly as an alter conversion derived from
-- the table's mutation history. A cross-table clone records the DESTINATION's metadata version, and
-- the destination's history has no such entry, so the conversion is lost and the affected column
-- reads as default (RENAME) or as stale pre-drop data (DROP + ADD), with no error and no row loss.
-- ATTACH/REPLACE PARTITION FROM and MOVE PARTITION TO TABLE must refuse such a source instead. A
-- conversion that no column of the tables reads through is not such a case, and is still cloned.
-- Continued in `05218_attach_partition_from_unapplied_metadata_mutation_2.sql`.

DROP TABLE IF EXISTS t_mvclone_src SYNC;
DROP TABLE IF EXISTS t_mvclone_dst SYNC;
DROP TABLE IF EXISTS t_mvclone_dst2 SYNC;
DROP TABLE IF EXISTS t_mvclone_dst_mt SYNC;
DROP TABLE IF EXISTS t_mvclone_mt_src SYNC;
DROP TABLE IF EXISTS t_mvclone_mt_dst SYNC;
DROP TABLE IF EXISTS t_mvclone_drop_src SYNC;
DROP TABLE IF EXISTS t_mvclone_drop_dst SYNC;
DROP TABLE IF EXISTS t_mvclone_swap_src SYNC;
DROP TABLE IF EXISTS t_mvclone_swap_dst SYNC;
DROP TABLE IF EXISTS t_mvclone_share_src SYNC;
DROP TABLE IF EXISTS t_mvclone_share_dst SYNC;
DROP TABLE IF EXISTS t_mvclone_marker_src SYNC;
DROP TABLE IF EXISTS t_mvclone_marker_dst SYNC;

-- ============ ReplicatedMergeTree source carrying a pending RENAME COLUMN ============
CREATE TABLE t_mvclone_src (id UInt64, a UInt32, p UInt8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mvclone_src', '1')
PARTITION BY p ORDER BY id;

INSERT INTO t_mvclone_src SELECT number, 1000 + number, 1 FROM numbers(10);

-- Detaching before the rename leaves no attached part to rewrite, so the returning part keeps its
-- own (older) metadata version and the rename stays an unapplied metadata mutation for it.
ALTER TABLE t_mvclone_src DETACH PARTITION 1;
ALTER TABLE t_mvclone_src RENAME COLUMN a TO b;
ALTER TABLE t_mvclone_src ATTACH PARTITION 1;

-- Keep the part at that version for the whole section: a merge or mutation would materialize the
-- conversion and the operations below would legitimately stop being refused.
SYSTEM STOP MERGES t_mvclone_src;

SELECT 'src reads the renamed column', count(), sum(b) FROM t_mvclone_src;

CREATE TABLE t_mvclone_dst (id UInt64, b UInt32, p UInt8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mvclone_dst', '1')
PARTITION BY p ORDER BY id;

ALTER TABLE t_mvclone_dst ATTACH PARTITION 1 FROM t_mvclone_src; -- { serverError SUPPORT_IS_DISABLED }
SELECT 'after refused ATTACH FROM: dst parts', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_mvclone_dst' AND active;

-- REPLACE would drop the destination's own rows first, so the refusal must happen before that.
CREATE TABLE t_mvclone_dst2 (id UInt64, b UInt32, p UInt8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mvclone_dst2', '1')
PARTITION BY p ORDER BY id;
INSERT INTO t_mvclone_dst2 SELECT number, 7, 1 FROM numbers(3);

ALTER TABLE t_mvclone_dst2 REPLACE PARTITION 1 FROM t_mvclone_src; -- { serverError SUPPORT_IS_DISABLED }
SELECT 'after refused REPLACE FROM: dst2 keeps its rows', count(), sum(b) FROM t_mvclone_dst2;

-- A plain MergeTree destination takes the StorageMergeTree code path instead. Ordered before the MOVE
-- below so that a regression in one guard cannot empty the source and mask the other.
CREATE TABLE t_mvclone_dst_mt (id UInt64, b UInt32, p UInt8)
ENGINE = MergeTree PARTITION BY p ORDER BY id;

ALTER TABLE t_mvclone_dst_mt ATTACH PARTITION 1 FROM t_mvclone_src; -- { serverError SUPPORT_IS_DISABLED }
SELECT 'after refused ATTACH FROM into MergeTree: dst parts', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_mvclone_dst_mt' AND active;

ALTER TABLE t_mvclone_src MOVE PARTITION 1 TO TABLE t_mvclone_dst; -- { serverError SUPPORT_IS_DISABLED }
SELECT 'after refused MOVE TO TABLE: src intact', count(), sum(b) FROM t_mvclone_src;

-- ============ control: the remediation the error message names must work ============
SYSTEM START MERGES t_mvclone_src;
OPTIMIZE TABLE t_mvclone_src PARTITION ID '1' FINAL;
ALTER TABLE t_mvclone_dst ATTACH PARTITION 1 FROM t_mvclone_src;
SELECT 'after OPTIMIZE FINAL the clone carries the renamed data', count(), sum(b) FROM t_mvclone_dst;

-- ============ plain MergeTree source carrying a pending RENAME COLUMN ============
-- Here the mutation itself is left unfinished (merges stopped, no sync), which is what keeps the
-- conversion live for the part on this engine.
CREATE TABLE t_mvclone_mt_src (id UInt64, a UInt32, p UInt8)
ENGINE = MergeTree PARTITION BY p ORDER BY id;
CREATE TABLE t_mvclone_mt_dst (id UInt64, b UInt32, p UInt8)
ENGINE = MergeTree PARTITION BY p ORDER BY id;

INSERT INTO t_mvclone_mt_src SELECT number, 1000 + number, 1 FROM numbers(10);
SYSTEM STOP MERGES t_mvclone_mt_src;
ALTER TABLE t_mvclone_mt_src RENAME COLUMN a TO b SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'MergeTree src reads the renamed column', count(), sum(b) FROM t_mvclone_mt_src;
ALTER TABLE t_mvclone_mt_src MOVE PARTITION 1 TO TABLE t_mvclone_mt_dst; -- { serverError SUPPORT_IS_DISABLED }
SELECT 'after refused MergeTree MOVE: src intact', count(), sum(b) FROM t_mvclone_mt_src;

-- ============ DROP COLUMN: the destination would resurrect the pre-drop values ============
CREATE TABLE t_mvclone_drop_src (id UInt64, c UInt32, p UInt8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mvclone_drop_src', '1')
PARTITION BY p ORDER BY id;

INSERT INTO t_mvclone_drop_src SELECT number, 1000 + number, 1 FROM numbers(10);
ALTER TABLE t_mvclone_drop_src DETACH PARTITION 1;
ALTER TABLE t_mvclone_drop_src DROP COLUMN c;
ALTER TABLE t_mvclone_drop_src ADD COLUMN c UInt32;
ALTER TABLE t_mvclone_drop_src ATTACH PARTITION 1;
SYSTEM STOP MERGES t_mvclone_drop_src;

SELECT 'src reads the dropped-and-readded column', count(), sum(c) FROM t_mvclone_drop_src;

CREATE TABLE t_mvclone_drop_dst (id UInt64, c UInt32, p UInt8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mvclone_drop_dst', '1')
PARTITION BY p ORDER BY id;

ALTER TABLE t_mvclone_drop_dst ATTACH PARTITION 1 FROM t_mvclone_drop_src; -- { serverError SUPPORT_IS_DISABLED }
SELECT 'after refused ATTACH FROM: drop dst parts', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_mvclone_drop_dst' AND active;

-- ============ RENAME onto a name the part also stores: the clone would read the other column ============
-- Dropping `b` before renaming `a` to `b` leaves the part holding both names, so the destination reads
-- the part's own stale `b` where the source reads the renamed `a`.
CREATE TABLE t_mvclone_swap_src (id UInt64, a UInt32, b UInt32, p UInt8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mvclone_swap_src', '1')
PARTITION BY p ORDER BY id;

INSERT INTO t_mvclone_swap_src SELECT number, 1000 + number, 7, 1 FROM numbers(10);
ALTER TABLE t_mvclone_swap_src DETACH PARTITION 1;
ALTER TABLE t_mvclone_swap_src DROP COLUMN b;
ALTER TABLE t_mvclone_swap_src RENAME COLUMN a TO b;
ALTER TABLE t_mvclone_swap_src ATTACH PARTITION 1;
SYSTEM STOP MERGES t_mvclone_swap_src;

SELECT 'src reads the renamed column and not the stale one', count(), sum(b) FROM t_mvclone_swap_src;

CREATE TABLE t_mvclone_swap_dst (id UInt64, b UInt32, p UInt8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mvclone_swap_dst', '1')
PARTITION BY p ORDER BY id;

ALTER TABLE t_mvclone_swap_dst ATTACH PARTITION 1 FROM t_mvclone_swap_src; -- { serverError SUPPORT_IS_DISABLED }
SELECT 'after refused ATTACH FROM: swap dst parts', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_mvclone_swap_dst' AND active;

-- ============ DROP COLUMN of a Nested parent, which shared offsets extend to its subcolumns ============
-- `share_nested_offsets` is on by default, so dropping `n` makes the part's `n.a` stale as well, and
-- re-adding the column gives the destination a name to read it under. The offsets are shared, so the
-- array lengths survive the masking and the values are what has to be read as defaults.
CREATE TABLE t_mvclone_share_src (id UInt64, n Nested(a UInt32), p UInt8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mvclone_share_src', '1')
PARTITION BY p ORDER BY id;

INSERT INTO t_mvclone_share_src SELECT number, [number], 1 FROM numbers(10);
ALTER TABLE t_mvclone_share_src DETACH PARTITION 1;
ALTER TABLE t_mvclone_share_src DROP COLUMN n;
ALTER TABLE t_mvclone_share_src ADD COLUMN n Nested(a UInt32);
ALTER TABLE t_mvclone_share_src ATTACH PARTITION 1;
SYSTEM STOP MERGES t_mvclone_share_src;

SELECT 'src reads the re-added nested column as defaults', count(), sum(arraySum(n.a)) FROM t_mvclone_share_src;

CREATE TABLE t_mvclone_share_dst (id UInt64, n Nested(a UInt32), p UInt8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mvclone_share_dst', '1')
PARTITION BY p ORDER BY id;

ALTER TABLE t_mvclone_share_dst ATTACH PARTITION 1 FROM t_mvclone_share_src; -- { serverError SUPPORT_IS_DISABLED }
SELECT 'after refused ATTACH FROM: share dst parts', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_mvclone_share_dst' AND active;

-- ============ a column skipped on insert is carried by a marker instead of by files ============
-- `skip_empty_columns_on_insert` writes no files for an all-default column and records its type in
-- `serialization.json` instead. The pending DROP invalidates that marker, so the source evaluates the
-- re-added DEFAULT where a clone would honour the marker and read the recorded type's default.
CREATE TABLE t_mvclone_marker_src (id UInt64, e UInt32, p UInt8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mvclone_marker_src', '1')
PARTITION BY p ORDER BY id
SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
         ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_mvclone_marker_src SELECT number, 0, 1 FROM numbers(10);
ALTER TABLE t_mvclone_marker_src DETACH PARTITION 1;
ALTER TABLE t_mvclone_marker_src DROP COLUMN e;
ALTER TABLE t_mvclone_marker_src ADD COLUMN e UInt32 DEFAULT 999;
ALTER TABLE t_mvclone_marker_src ATTACH PARTITION 1;
SYSTEM STOP MERGES t_mvclone_marker_src;

SELECT 'the skipped column has no files in the part', countIf(column = 'e') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_mvclone_marker_src' AND active;
SELECT 'src reads the re-added default and not the marker', count(), sum(e) FROM t_mvclone_marker_src;

CREATE TABLE t_mvclone_marker_dst (id UInt64, e UInt32 DEFAULT 999, p UInt8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mvclone_marker_dst', '1')
PARTITION BY p ORDER BY id
SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
         ratio_of_defaults_for_sparse_serialization = 1.0;

ALTER TABLE t_mvclone_marker_dst ATTACH PARTITION 1 FROM t_mvclone_marker_src; -- { serverError SUPPORT_IS_DISABLED }
SELECT 'after refused ATTACH FROM: marker dst parts', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_mvclone_marker_dst' AND active;

DROP TABLE t_mvclone_src SYNC;
DROP TABLE t_mvclone_dst SYNC;
DROP TABLE t_mvclone_dst2 SYNC;
DROP TABLE t_mvclone_dst_mt SYNC;
DROP TABLE t_mvclone_mt_src SYNC;
DROP TABLE t_mvclone_mt_dst SYNC;
DROP TABLE t_mvclone_drop_src SYNC;
DROP TABLE t_mvclone_drop_dst SYNC;
DROP TABLE t_mvclone_swap_src SYNC;
DROP TABLE t_mvclone_swap_dst SYNC;
DROP TABLE t_mvclone_share_src SYNC;
DROP TABLE t_mvclone_share_dst SYNC;
DROP TABLE t_mvclone_marker_src SYNC;
DROP TABLE t_mvclone_marker_dst SYNC;
