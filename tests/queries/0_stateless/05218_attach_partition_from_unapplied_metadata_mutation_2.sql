-- Tags: zookeeper, no-replicated-database
-- zookeeper: most sources below are `ReplicatedMergeTree`, which needs Keeper.
-- no-replicated-database: the oracles below count parts and sum columns per table, which the extra
-- shard perturbs (same reason as 03100_lwu_51_replace_partition_pending_patch on this code path).

-- Continues `05218_attach_partition_from_unapplied_metadata_mutation.sql`, which states what the
-- refusal is for.

DROP TABLE IF EXISTS t_mvclone_mark2_src SYNC;
DROP TABLE IF EXISTS t_mvclone_mark2_dst SYNC;
DROP TABLE IF EXISTS t_mvclone_clear_src SYNC;
DROP TABLE IF EXISTS t_mvclone_clear_dst SYNC;
DROP TABLE IF EXISTS t_mvclone_gone_src SYNC;
DROP TABLE IF EXISTS t_mvclone_gone_dst SYNC;
DROP TABLE IF EXISTS t_mvclone_late_src SYNC;
DROP TABLE IF EXISTS t_mvclone_late_dst SYNC;
DROP TABLE IF EXISTS t_mvclone_nested_src SYNC;
DROP TABLE IF EXISTS t_mvclone_nested_dst SYNC;
DROP TABLE IF EXISTS t_mvclone_clean_src SYNC;
DROP TABLE IF EXISTS t_mvclone_clean_dst SYNC;
DROP TABLE IF EXISTS t_mvclone_read_src SYNC;
DROP TABLE IF EXISTS t_mvclone_read_dst SYNC;

-- ============ control: a DROP COLUMN that no schema names any more must not be refused ============
-- Without the re-ADD above, the column is in neither table, so no read resolves through the conversion and
-- losing it changes nothing. `05210_materialize_ttl_of_attached_part_with_dropped_column` clones exactly this
-- partition to reach the state it covers.
CREATE TABLE t_mvclone_gone_src (id UInt64, c UInt32, p UInt8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mvclone_gone_src', '1')
PARTITION BY p ORDER BY id;

INSERT INTO t_mvclone_gone_src SELECT number, 1000 + number, 1 FROM numbers(10);
ALTER TABLE t_mvclone_gone_src DETACH PARTITION 1;
ALTER TABLE t_mvclone_gone_src DROP COLUMN c;
ALTER TABLE t_mvclone_gone_src ATTACH PARTITION 1;
SYSTEM STOP MERGES t_mvclone_gone_src;

-- The DROP moved the table's version while the returning part kept its own, and the part still holds the
-- column: that pair is what makes the conversion live for it, so the clone below really does lose one.
SELECT 'the dropping table moved its metadata version', metadata_version FROM system.tables
WHERE database = currentDatabase() AND name = 't_mvclone_gone_src';
SELECT 'the source part still holds the dropped column', countIf(column = 'c') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_mvclone_gone_src' AND active;

CREATE TABLE t_mvclone_gone_dst (id UInt64, p UInt8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mvclone_gone_dst', '1')
PARTITION BY p ORDER BY id;

ALTER TABLE t_mvclone_gone_dst ATTACH PARTITION 1 FROM t_mvclone_gone_src;
SELECT 'a part whose dropped column no schema has is cloned', count(), sum(id) FROM t_mvclone_gone_dst;

-- ============ control: a rename of a column the part never stored must not be refused ============
-- The part predates the `ADD COLUMN`, so the source reads the renamed column as a default too and the
-- clone reads exactly what the source reads.
CREATE TABLE t_mvclone_late_src (id UInt64, p UInt8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mvclone_late_src', '1')
PARTITION BY p ORDER BY id;

INSERT INTO t_mvclone_late_src SELECT number, 1 FROM numbers(10);
ALTER TABLE t_mvclone_late_src ADD COLUMN a UInt32;
ALTER TABLE t_mvclone_late_src DETACH PARTITION 1;
ALTER TABLE t_mvclone_late_src RENAME COLUMN a TO b;
ALTER TABLE t_mvclone_late_src ATTACH PARTITION 1;
SYSTEM STOP MERGES t_mvclone_late_src;

SELECT 'src reads the never-written renamed column', count(), sum(b) FROM t_mvclone_late_src;

CREATE TABLE t_mvclone_late_dst (id UInt64, p UInt8, b UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mvclone_late_dst', '1')
PARTITION BY p ORDER BY id;

ALTER TABLE t_mvclone_late_dst ATTACH PARTITION 1 FROM t_mvclone_late_src;
SELECT 'the clone reads the renamed column the same way', count(), sum(b) FROM t_mvclone_late_dst;

-- ============ control: `share_nested_offsets = 0` makes a dotted name independent ============
-- With the setting off, dropping `n` leaves `n.a` alone, so the part's `n.a` is what both tables read
-- and the clone must be allowed. With it on, the same drop would reach `n.a` and be refused.
CREATE TABLE t_mvclone_nested_src (id UInt64, `n.a` Array(UInt32), p UInt8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mvclone_nested_src', '1')
PARTITION BY p ORDER BY id SETTINGS share_nested_offsets = 0;

ALTER TABLE t_mvclone_nested_src ADD COLUMN n String;
INSERT INTO t_mvclone_nested_src SELECT number, [number], 1, 'x' FROM numbers(10);
ALTER TABLE t_mvclone_nested_src DETACH PARTITION 1;
ALTER TABLE t_mvclone_nested_src DROP COLUMN n;
ALTER TABLE t_mvclone_nested_src ATTACH PARTITION 1;
SYSTEM STOP MERGES t_mvclone_nested_src;

CREATE TABLE t_mvclone_nested_dst (id UInt64, `n.a` Array(UInt32), p UInt8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mvclone_nested_dst', '1')
PARTITION BY p ORDER BY id SETTINGS share_nested_offsets = 0;

ALTER TABLE t_mvclone_nested_dst ATTACH PARTITION 1 FROM t_mvclone_nested_src;
SELECT 'an independent dotted column survives its prefix being dropped', count(), sum(`n.a`[1])
FROM t_mvclone_nested_dst;

-- ============ control: a marker that the dropped current name invalidates must not be refused ============
-- The drop of `b` invalidates the marker standing in for `a` as well, so the source falls back to the
-- re-declared `DEFAULT` and the destination, which has no carrier for `b` at all, reads the same value.
CREATE TABLE t_mvclone_mark2_src (id UInt64, a UInt32, p UInt8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mvclone_mark2_src', '1')
PARTITION BY p ORDER BY id
SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
         ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_mvclone_mark2_src SELECT number, 0, 1 FROM numbers(10);
ALTER TABLE t_mvclone_mark2_src ADD COLUMN b UInt32;
ALTER TABLE t_mvclone_mark2_src DETACH PARTITION 1;
ALTER TABLE t_mvclone_mark2_src DROP COLUMN b;
ALTER TABLE t_mvclone_mark2_src RENAME COLUMN a TO b;
ALTER TABLE t_mvclone_mark2_src MODIFY COLUMN b UInt32 DEFAULT 555;
ALTER TABLE t_mvclone_mark2_src ATTACH PARTITION 1;
SYSTEM STOP MERGES t_mvclone_mark2_src;

SELECT 'src reads the default that the invalidated marker left', count(), sum(b) FROM t_mvclone_mark2_src;

CREATE TABLE t_mvclone_mark2_dst (id UInt64, b UInt32 DEFAULT 555, p UInt8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mvclone_mark2_dst', '1')
PARTITION BY p ORDER BY id
SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
         ratio_of_defaults_for_sparse_serialization = 1.0;

ALTER TABLE t_mvclone_mark2_dst ATTACH PARTITION 1 FROM t_mvclone_mark2_src;
SELECT 'the clone reads that same default', count(), sum(b) FROM t_mvclone_mark2_dst;

-- ============ CLEAR COLUMN masks the part the same way and must be refused as well ============
-- It is a `DROP_COLUMN` carrying `clear`, so the destination would read the values the source masks.
-- Merges stay stopped for the whole arm: the mutation is what discharges the command, and the refusal
-- names it instead of OPTIMIZE, which only writes the defaults into a merged part.
CREATE TABLE t_mvclone_clear_src (id UInt64, c UInt32, p UInt8) ENGINE = MergeTree PARTITION BY p ORDER BY id;
CREATE TABLE t_mvclone_clear_dst (id UInt64, c UInt32, p UInt8) ENGINE = MergeTree PARTITION BY p ORDER BY id;

INSERT INTO t_mvclone_clear_src SELECT number, 1000 + number, 1 FROM numbers(10);
SYSTEM STOP MERGES t_mvclone_clear_src;
ALTER TABLE t_mvclone_clear_src CLEAR COLUMN c IN PARTITION 1 SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'src reads the cleared column as defaults', count(), sum(c) FROM t_mvclone_clear_src;
ALTER TABLE t_mvclone_clear_dst ATTACH PARTITION 1 FROM t_mvclone_clear_src; -- { serverError SUPPORT_IS_DISABLED }
SELECT 'after refused ATTACH FROM: clear dst parts', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_mvclone_clear_dst' AND active;

-- ============ control: a source with nothing pending must not be refused ============
CREATE TABLE t_mvclone_clean_src (id UInt64, b UInt32, p UInt8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mvclone_clean_src', '1')
PARTITION BY p ORDER BY id;
CREATE TABLE t_mvclone_clean_dst (id UInt64, b UInt32, p UInt8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mvclone_clean_dst', '1')
PARTITION BY p ORDER BY id;

INSERT INTO t_mvclone_clean_src SELECT number, 1000 + number, 1 FROM numbers(10);
ALTER TABLE t_mvclone_clean_dst ATTACH PARTITION 1 FROM t_mvclone_clean_src;
SELECT 'clean source is cloned', count(), sum(b) FROM t_mvclone_clean_dst;

-- ============ control: the deliberately excluded sibling class (ALTER MODIFY COLUMN) ============
-- READ_COLUMN is keyed on the same part-vs-table metadata version but is not refused: the retype is
-- applied from the part's own columns.txt, so it survives a clone. The part below is level with a
-- finished RENAME COLUMN and behind a pending MODIFY COLUMN, so the clone must be allowed.
CREATE TABLE t_mvclone_read_src (id UInt64, a UInt32, p UInt8) ENGINE = MergeTree PARTITION BY p ORDER BY id;
INSERT INTO t_mvclone_read_src SELECT number, 1000 + number, 1 FROM numbers(10);
ALTER TABLE t_mvclone_read_src RENAME COLUMN a TO b SETTINGS mutations_sync = 2;
SYSTEM STOP MERGES t_mvclone_read_src;
ALTER TABLE t_mvclone_read_src MODIFY COLUMN b UInt64 SETTINGS mutations_sync = 0, alter_sync = 0;

CREATE TABLE t_mvclone_read_dst (id UInt64, b UInt64, p UInt8) ENGINE = MergeTree PARTITION BY p ORDER BY id;
ALTER TABLE t_mvclone_read_dst ATTACH PARTITION 1 FROM t_mvclone_read_src;
SELECT 'a part behind only ALTER MODIFY COLUMN is cloned', count(), sum(b) FROM t_mvclone_read_dst;

DROP TABLE t_mvclone_mark2_src SYNC;
DROP TABLE t_mvclone_mark2_dst SYNC;
DROP TABLE t_mvclone_clear_src SYNC;
DROP TABLE t_mvclone_clear_dst SYNC;
DROP TABLE t_mvclone_gone_src SYNC;
DROP TABLE t_mvclone_gone_dst SYNC;
DROP TABLE t_mvclone_late_src SYNC;
DROP TABLE t_mvclone_late_dst SYNC;
DROP TABLE t_mvclone_nested_src SYNC;
DROP TABLE t_mvclone_nested_dst SYNC;
DROP TABLE t_mvclone_clean_src SYNC;
DROP TABLE t_mvclone_clean_dst SYNC;
DROP TABLE t_mvclone_read_src SYNC;
DROP TABLE t_mvclone_read_dst SYNC;
