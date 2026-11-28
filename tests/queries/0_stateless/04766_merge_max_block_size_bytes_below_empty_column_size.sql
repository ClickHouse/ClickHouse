-- Tags: no-random-merge-tree-settings, no-random-settings

-- Regression test for issue #113475: an empty `LowCardinality(String)` column already
-- carries its dictionary, so `MergedData::hasEnoughRows()` reported enough bytes before
-- a single row was merged and `OPTIMIZE FINAL` spun forever at 100% CPU.

DROP TABLE IF EXISTS t_merge_spin;

-- `merge_max_block_size` is pinned here because the test runner randomizes it; a value
-- set in the DDL wins over the runner's injection.
CREATE TABLE t_merge_spin (a UInt64, b LowCardinality(String))
ENGINE = MergeTree
ORDER BY a
SETTINGS merge_max_block_size = 256, merge_max_block_size_bytes = 8;

-- Overlapping ranges, so the merge must interleave both parts instead of copying one.
INSERT INTO t_merge_spin SELECT number, '' FROM numbers(10);
INSERT INTO t_merge_spin SELECT number + 5, '' FROM numbers(10);

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_spin' AND active;

OPTIMIZE TABLE t_merge_spin FINAL;

-- Without the fix `OPTIMIZE FINAL` never returns. All 20 rows must survive, including the
-- duplicated keys 5-9 contributed by both parts.
SELECT count(), sum(a) FROM t_merge_spin;
SELECT a FROM t_merge_spin ORDER BY a;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_spin' AND active;

DROP TABLE t_merge_spin;
