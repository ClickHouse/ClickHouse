-- While a `RENAME COLUMN` is still a pending metadata mutation, reads apply the rename on the fly, but
-- the part's statistics are stored under the on-disk names. Looking an estimate up by the queried name
-- then describes a different column's data, so such a part must not be pruned.

DROP TABLE IF EXISTS t_statistics_pending_rename;
CREATE TABLE t_statistics_pending_rename (x Int64 STATISTICS(basic), y Int64 STATISTICS(basic)) ENGINE = MergeTree ORDER BY tuple();
-- Only to hold the mutation pending deterministically.
SYSTEM STOP MERGES t_statistics_pending_rename;
INSERT INTO t_statistics_pending_rename VALUES (1, 100), (2, 200);

ALTER TABLE t_statistics_pending_rename DROP COLUMN x, RENAME COLUMN y TO x SETTINGS alter_sync = 0, mutations_sync = 0;

-- The rename is already applied to reads.
SELECT groupArray(x) FROM t_statistics_pending_rename;
SELECT count(), (SELECT count() FROM t_statistics_pending_rename WHERE x = 100 SETTINGS use_statistics_for_part_pruning = 0) FROM t_statistics_pending_rename WHERE x = 100;
-- A value inside the dropped column's range must not match either.
SELECT count(), (SELECT count() FROM t_statistics_pending_rename WHERE x = 1 SETTINGS use_statistics_for_part_pruning = 0) FROM t_statistics_pending_rename WHERE x = 1;

SYSTEM START MERGES t_statistics_pending_rename;
DROP TABLE t_statistics_pending_rename;

-- Without a pending rename a part is still pruned by its statistics.
DROP TABLE IF EXISTS t_statistics_pruned;
CREATE TABLE t_statistics_pruned (x Int64 STATISTICS(basic)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_statistics_pruned VALUES (1), (2);
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_statistics_pruned WHERE x = 100) WHERE explain LIKE '%Parts: 0/1%';
DROP TABLE t_statistics_pruned;
