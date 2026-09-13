-- While a metadata mutation that changes column identity is still pending, reads already apply it,
-- but the part's statistics are stored under the on-disk names. Looking an estimate up by the
-- queried name then describes other (or stale) data, so such a part must not be pruned.

DROP TABLE IF EXISTS t_statistics_pending_rename;
CREATE TABLE t_statistics_pending_rename (x Int64 STATISTICS(basic), y Int64 STATISTICS(basic)) ENGINE = MergeTree ORDER BY tuple();
-- Only to hold the mutation pending deterministically.
SYSTEM STOP MERGES t_statistics_pending_rename;
INSERT INTO t_statistics_pending_rename SETTINGS materialize_statistics_on_insert = 1 VALUES (1, 100), (2, 200);

ALTER TABLE t_statistics_pending_rename DROP COLUMN x, RENAME COLUMN y TO x SETTINGS alter_sync = 0, mutations_sync = 0;

-- The rename is already applied to reads.
SELECT groupArray(x) FROM t_statistics_pending_rename;
SELECT count(), (SELECT count() FROM t_statistics_pending_rename WHERE x = 100 SETTINGS use_statistics_for_part_pruning = 0) FROM t_statistics_pending_rename WHERE x = 100;
-- A value inside the dropped column's range must not match either.
SELECT count(), (SELECT count() FROM t_statistics_pending_rename WHERE x = 1 SETTINGS use_statistics_for_part_pruning = 0) FROM t_statistics_pending_rename WHERE x = 1;

SYSTEM START MERGES t_statistics_pending_rename;
DROP TABLE t_statistics_pending_rename;

-- The same holds for a pending `DROP COLUMN` followed by re-adding a column with the same name:
-- reads treat the on-disk data as missing and fill the new default, while the part's statistics
-- still describe the dropped column's data.
DROP TABLE IF EXISTS t_statistics_pending_readd;
CREATE TABLE t_statistics_pending_readd (id Int64, x Int64 STATISTICS(basic)) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES t_statistics_pending_readd;
INSERT INTO t_statistics_pending_readd SETTINGS materialize_statistics_on_insert = 1 VALUES (1, 1), (2, 2);

ALTER TABLE t_statistics_pending_readd DROP COLUMN x, ADD COLUMN x Int64 DEFAULT 100 SETTINGS alter_sync = 0, mutations_sync = 0;

SELECT groupArray(x) FROM t_statistics_pending_readd;
SELECT count(), (SELECT count() FROM t_statistics_pending_readd WHERE x = 100 SETTINGS use_statistics_for_part_pruning = 0) FROM t_statistics_pending_readd WHERE x = 100;
-- A value inside the dropped column's range must not match either.
SELECT count(), (SELECT count() FROM t_statistics_pending_readd WHERE x = 1 SETTINGS use_statistics_for_part_pruning = 0) FROM t_statistics_pending_readd WHERE x = 1;

SYSTEM START MERGES t_statistics_pending_readd;
DROP TABLE t_statistics_pending_readd;

-- Without a pending metadata mutation a part is still pruned by its statistics.
DROP TABLE IF EXISTS t_statistics_pruned;
CREATE TABLE t_statistics_pruned (x Int64 STATISTICS(basic)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_statistics_pruned SETTINGS materialize_statistics_on_insert = 1 VALUES (1), (2);
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_statistics_pruned WHERE x = 100) WHERE explain LIKE '%Parts: 0/1%';
DROP TABLE t_statistics_pruned;
