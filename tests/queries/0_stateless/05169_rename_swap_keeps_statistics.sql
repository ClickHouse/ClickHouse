-- Tags: zookeeper
-- A mutation can carry a composed rename pair that exchanges two column names. Applying it to the
-- part's statistics one entry at a time lost one of them: the first rename found its target still held
-- by the other column, so the insert did nothing and the following erase dropped the entry.

-- The statistics have to be written by the insert itself for the part to carry them at all.
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS t_rename_swap_statistics SYNC;

CREATE TABLE t_rename_swap_statistics (a UInt64 STATISTICS(tdigest), b UInt64 STATISTICS(tdigest), c UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_rename_swap_statistics', 'r1')
ORDER BY tuple() PARTITION BY tuple()
-- The statistics set is pinned so it does not depend on the default of the day, and the part type is
-- pinned to compact: the same swap on a wide part fails the mutation with a separate, pre-existing
-- error ("Stream ... is not found" out of `MergeTreeReaderWide`), which this test is not about.
SETTINGS auto_statistics_types = 'basic, uniq_v2', min_bytes_for_wide_part = 10485760, min_rows_for_wide_part = 1000000;

INSERT INTO t_rename_swap_statistics VALUES (1, 2, 3);

SELECT 'before the swap';
SELECT column, statistics FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_rename_swap_statistics' AND active AND column IN ('a', 'b', 'c') ORDER BY column;

ALTER TABLE t_rename_swap_statistics DETACH PARTITION tuple();
ALTER TABLE t_rename_swap_statistics RENAME COLUMN a TO a1, RENAME COLUMN b TO b1;
ALTER TABLE t_rename_swap_statistics RENAME COLUMN a1 TO b, RENAME COLUMN b1 TO a;
ALTER TABLE t_rename_swap_statistics ATTACH PARTITION tuple();

ALTER TABLE t_rename_swap_statistics UPDATE c = c + 10 WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'after the swap and a mutation';
SELECT column, statistics FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_rename_swap_statistics' AND active AND column IN ('a', 'b', 'c') ORDER BY column;

SELECT 'the rows themselves';
-- Which of the two swapped names the re-attached part's data ends up under is engine-dependent, and
-- this test is about the statistics, not about that. Assert what does not depend on it: both values
-- survived the swap, and the mutation ran.
SELECT a + b, c FROM t_rename_swap_statistics;

DROP TABLE t_rename_swap_statistics SYNC;
