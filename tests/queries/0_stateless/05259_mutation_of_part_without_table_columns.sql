-- Tags: zookeeper, no-fasttest, no-shared-merge-tree, no-replicated-database
-- zookeeper: the replicated arm needs Keeper.
-- no-fasttest: waiting for the failed replicated mutation takes about 30 seconds:
--   https://github.com/ClickHouse/ClickHouse/issues/67936
-- no-shared-merge-tree: the arms rely on how `MergeTree` and `ReplicatedMergeTree` report and kill a
--   failed mutation.
-- no-replicated-database: the replicated arm names its Keeper path explicitly, which a `Replicated`
--   database rejects (`database_replicated_allow_replicated_engine_arguments` defaults to 0).

-- A part can hold only columns the table no longer has: here a partition is detached, its only column
-- is replaced by a new one, and the partition is re-attached. Reads of such a part fail with
-- `NO_SUCH_COLUMN_IN_TABLE` and keep it, so its data can be recovered by adding the column back. A
-- mutation that reads nothing from the part (`MODIFY COLUMN`, `RENAME COLUMN` of the new column) must
-- fail the same way and keep the part too, not write a part with no columns, which can be neither read
-- nor loaded. Block number and offset columns are disabled, because they would be kept in the part.

DROP TABLE IF EXISTS t_compact;
DROP TABLE IF EXISTS t_wide;
DROP TABLE IF EXISTS t_replicated;
DROP TABLE IF EXISTS t_two_columns;

CREATE TABLE t_compact (v UInt32) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_compact VALUES (1);
ALTER TABLE t_compact DETACH PARTITION ALL;
ALTER TABLE t_compact ADD COLUMN w UInt32;
ALTER TABLE t_compact DROP COLUMN v;
ALTER TABLE t_compact ATTACH PARTITION ALL;

SELECT 'compact', part_type, arraySort(groupArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_compact' AND active GROUP BY part_type;
ALTER TABLE t_compact MODIFY COLUMN w UInt64 SETTINGS mutations_sync = 2; -- { serverError UNFINISHED }
SELECT 'compact', latest_fail_reason LIKE '%NO_SUCH_COLUMN_IN_TABLE%' FROM system.mutations
WHERE database = currentDatabase() AND table = 't_compact' AND NOT is_done;
SELECT 'compact', part_type, arraySort(groupArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_compact' AND active GROUP BY part_type;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_compact' FORMAT Null;
ALTER TABLE t_compact ADD COLUMN v UInt32;
SELECT 'compact', v, w FROM t_compact;

CREATE TABLE t_wide (v UInt32) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_wide VALUES (1);
ALTER TABLE t_wide DETACH PARTITION ALL;
ALTER TABLE t_wide ADD COLUMN w UInt32;
ALTER TABLE t_wide DROP COLUMN v;
ALTER TABLE t_wide ATTACH PARTITION ALL;

SELECT 'wide', part_type, arraySort(groupArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_wide' AND active GROUP BY part_type;
ALTER TABLE t_wide MODIFY COLUMN w UInt64 SETTINGS mutations_sync = 2; -- { serverError UNFINISHED }
SELECT 'wide', latest_fail_reason LIKE '%NO_SUCH_COLUMN_IN_TABLE%' FROM system.mutations
WHERE database = currentDatabase() AND table = 't_wide' AND NOT is_done;
SELECT 'wide', part_type, arraySort(groupArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_wide' AND active GROUP BY part_type;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_wide' FORMAT Null;
ALTER TABLE t_wide RENAME COLUMN w TO z SETTINGS mutations_sync = 2; -- { serverError UNFINISHED }
SELECT 'wide', latest_fail_reason LIKE '%NO_SUCH_COLUMN_IN_TABLE%' FROM system.mutations
WHERE database = currentDatabase() AND table = 't_wide' AND NOT is_done;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_wide' FORMAT Null;
ALTER TABLE t_wide ADD COLUMN v UInt32;
SELECT 'wide', v, z FROM t_wide;

CREATE TABLE t_replicated (v UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_replicated', '1') ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_replicated VALUES (1);
ALTER TABLE t_replicated DETACH PARTITION ALL;
ALTER TABLE t_replicated ADD COLUMN w UInt32;
ALTER TABLE t_replicated DROP COLUMN v;
ALTER TABLE t_replicated ATTACH PARTITION ALL;

SELECT 'replicated', part_type, arraySort(groupArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_replicated' AND active GROUP BY part_type;
ALTER TABLE t_replicated MODIFY COLUMN w UInt64 SETTINGS mutations_sync = 2; -- { serverError UNFINISHED }
SELECT 'replicated', latest_fail_reason LIKE '%NO_SUCH_COLUMN_IN_TABLE%' FROM system.mutations
WHERE database = currentDatabase() AND table = 't_replicated' AND NOT is_done;
SELECT 'replicated', part_type, arraySort(groupArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_replicated' AND active GROUP BY part_type;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_replicated' FORMAT Null;
ALTER TABLE t_replicated ADD COLUMN v UInt32;
SELECT 'replicated', v, w FROM t_replicated;

-- The same with a second column that the table still has: the mutation keeps that column and drops
-- the one the table does not have.
CREATE TABLE t_two_columns (v UInt32, x UInt32) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_two_columns VALUES (1, 10);
ALTER TABLE t_two_columns DETACH PARTITION ALL;
ALTER TABLE t_two_columns ADD COLUMN w UInt32;
ALTER TABLE t_two_columns DROP COLUMN v;
ALTER TABLE t_two_columns ATTACH PARTITION ALL;

SELECT 'two columns', part_type, arraySort(groupArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_two_columns' AND active GROUP BY part_type;
ALTER TABLE t_two_columns MODIFY COLUMN w UInt64 SETTINGS mutations_sync = 2;
SELECT 'two columns', count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_two_columns' AND NOT is_done;
SELECT 'two columns', part_type, arraySort(groupArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_two_columns' AND active GROUP BY part_type;
SELECT 'two columns', x, w FROM t_two_columns;

DROP TABLE t_compact;
DROP TABLE t_wide;
DROP TABLE t_replicated;
DROP TABLE t_two_columns;
