-- Regression test: `basic` statistics `default_count` and null count must survive a real
-- MergeTree part merge.  Before the fix, `StatisticsBasic::merge` applied the conjunction
-- `has_default_count && other->has_default_count` unconditionally, so merging built statistics
-- into the empty accumulator created by `ColumnsStatistics(metadata)` in `MergeTask` always
-- produced `has_default_count = false`, losing the count for the merged part.
--
-- We verify correctness via `system.parts_columns.estimates.default_count`, which is NULL when
-- `has_default_count` is false and carries the actual count when it is true.

DROP TABLE IF EXISTS t_stats_merge_regression;

CREATE TABLE t_stats_merge_regression
(
    id       UInt32,
    col      Int32             STATISTICS(basic),  -- non-Nullable: default is 0
    nullable Nullable(Int32)   STATISTICS(basic)   -- Nullable: default is NULL
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;  -- always write wide parts so statistics files are written

SET materialize_statistics_on_insert = 1;

SYSTEM STOP MERGES t_stats_merge_regression;

-- Part 1: 10 rows — 3 zeros in `col`, 3 NULLs in `nullable`.
INSERT INTO t_stats_merge_regression
    SELECT number,
           if(number < 3, 0, toInt32(number)),
           if(number < 3, NULL, toInt32(number))
    FROM numbers(10);

-- Part 2: another 10 rows — 4 zeros in `col`, 4 NULLs in `nullable`.
INSERT INTO t_stats_merge_regression
    SELECT 10 + number,
           if(number < 4, 0, toInt32(number)),
           if(number < 4, NULL, toInt32(number))
    FROM numbers(10);

-- Sanity: two active parts, each carrying statistics (built on insert).
SELECT count() FROM system.parts
WHERE table = 't_stats_merge_regression' AND active AND database = currentDatabase();

SELECT column, estimates.default_count
FROM system.parts_columns
WHERE table = 't_stats_merge_regression' AND active AND database = currentDatabase()
  AND column IN ('col', 'nullable')
ORDER BY column, estimates.default_count;

SYSTEM START MERGES t_stats_merge_regression;
OPTIMIZE TABLE t_stats_merge_regression FINAL;

-- After merge: one active part.
SELECT count() FROM system.parts
WHERE table = 't_stats_merge_regression' AND active AND database = currentDatabase();

-- The merged part must show the summed default counts.
-- `col`:      3 + 4 = 7 rows with value 0
-- `nullable`: 3 + 4 = 7 NULL rows
-- Without the fix both would be NULL here (has_default_count lost in the merge).
SELECT column, estimates.default_count
FROM system.parts_columns
WHERE table = 't_stats_merge_regression' AND active AND database = currentDatabase()
  AND column IN ('col', 'nullable')
ORDER BY column;

DROP TABLE t_stats_merge_regression;
