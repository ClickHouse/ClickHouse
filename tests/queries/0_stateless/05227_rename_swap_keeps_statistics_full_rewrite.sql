-- Tags: zookeeper
-- A compact part is always mutated by rewriting every column, and on that path the statistics
-- collectors are created from the current table metadata, so they already carry the names the
-- columns have now. Applying the part's pending `RENAME COLUMN` / `DROP COLUMN` commands to them a
-- second time moved the collectors onto the wrong columns: a swap of differently-typed columns
-- dropped both, because each collector was then typed for the other column. The second table covers
-- a neighbouring shape on the same path, a drop and a rename onto the dropped name.

SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS t_swap_types_statistics SYNC;

-- The two columns have different types: same-typed halves of a swap happen to land on a column of
-- the type they were built for, which hides the problem.
CREATE TABLE t_swap_types_statistics (a UInt64 STATISTICS(tdigest), b Float64 STATISTICS(tdigest), c UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_swap_types_statistics', 'r1')
ORDER BY tuple() PARTITION BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2', min_bytes_for_wide_part = 10485760, min_rows_for_wide_part = 1000000;

INSERT INTO t_swap_types_statistics VALUES (1, 2.5, 3);

SELECT 'before the swap';
SELECT column, type, statistics FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_swap_types_statistics' AND active AND column IN ('a', 'b', 'c') ORDER BY column;

ALTER TABLE t_swap_types_statistics DETACH PARTITION tuple();
ALTER TABLE t_swap_types_statistics RENAME COLUMN a TO a1, RENAME COLUMN b TO b1;
ALTER TABLE t_swap_types_statistics RENAME COLUMN a1 TO b, RENAME COLUMN b1 TO a;
ALTER TABLE t_swap_types_statistics ATTACH PARTITION tuple();

ALTER TABLE t_swap_types_statistics UPDATE c = c + 10 WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'after the swap and a mutation';
SELECT column, type, statistics FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_swap_types_statistics' AND active AND column IN ('a', 'b', 'c') ORDER BY column;

DROP TABLE t_swap_types_statistics SYNC;

SELECT 'a drop and a rename onto the dropped name';

DROP TABLE IF EXISTS t_drop_rename_statistics SYNC;

CREATE TABLE t_drop_rename_statistics (a UInt64 STATISTICS(tdigest), b UInt64 STATISTICS(tdigest), c UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_drop_rename_statistics', 'r1')
ORDER BY tuple() PARTITION BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2', min_bytes_for_wide_part = 10485760, min_rows_for_wide_part = 1000000;

INSERT INTO t_drop_rename_statistics VALUES (1, 2, 3);

ALTER TABLE t_drop_rename_statistics DETACH PARTITION tuple();
ALTER TABLE t_drop_rename_statistics DROP COLUMN a, RENAME COLUMN b TO a;
ALTER TABLE t_drop_rename_statistics ATTACH PARTITION tuple();

ALTER TABLE t_drop_rename_statistics UPDATE c = c + 10 WHERE 1 SETTINGS mutations_sync = 2;

SELECT column, type, statistics FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_drop_rename_statistics' AND active AND column IN ('a', 'b', 'c') ORDER BY column;

SELECT a, c FROM t_drop_rename_statistics;

DROP TABLE t_drop_rename_statistics SYNC;
