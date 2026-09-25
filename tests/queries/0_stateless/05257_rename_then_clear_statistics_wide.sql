-- Tags: zookeeper
-- A wide part mutated in place carries its untouched columns' statistics over, keyed by the names the
-- columns had in that part. The renames that bring the part up to date come after the mutation's own
-- commands, so a `CLEAR STATISTICS` of a column's current name found nothing to clear, and the later
-- rename put the old statistics back under the name the user had just cleared.

SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS t_rename_clear_statistics SYNC;

CREATE TABLE t_rename_clear_statistics (a UInt64 STATISTICS(tdigest), b UInt64 STATISTICS(tdigest), c UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_rename_clear_statistics', 'r1')
ORDER BY tuple() PARTITION BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_rename_clear_statistics VALUES (1, 2, 3);

SELECT 'before';
SELECT column, statistics FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_rename_clear_statistics' AND active AND column IN ('a', 'b', 'a1', 'c') ORDER BY column;

-- The detached part keeps its old metadata version, so the rename reaches it only through the
-- rename map of the next mutation, which is the `CLEAR STATISTICS` below.
ALTER TABLE t_rename_clear_statistics DETACH PARTITION tuple();
ALTER TABLE t_rename_clear_statistics RENAME COLUMN a TO a1;
ALTER TABLE t_rename_clear_statistics ATTACH PARTITION tuple();

ALTER TABLE t_rename_clear_statistics CLEAR STATISTICS a1 SETTINGS mutations_sync = 2;

SELECT 'after the rename and CLEAR STATISTICS of the new name';
SELECT column, statistics FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_rename_clear_statistics' AND active AND column IN ('a', 'b', 'a1', 'c') ORDER BY column;

ALTER TABLE t_rename_clear_statistics DETACH PARTITION tuple();
ALTER TABLE t_rename_clear_statistics RENAME COLUMN b TO b1;
ALTER TABLE t_rename_clear_statistics ATTACH PARTITION tuple();

ALTER TABLE t_rename_clear_statistics CLEAR STATISTICS ALL SETTINGS mutations_sync = 2;

SELECT 'after the rename and CLEAR STATISTICS ALL';
SELECT column, statistics FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_rename_clear_statistics' AND active AND column IN ('a1', 'b', 'b1', 'c') ORDER BY column;

SELECT a1, b1, c FROM t_rename_clear_statistics;

DROP TABLE t_rename_clear_statistics SYNC;
