-- Tags: no-random-settings, no-random-merge-tree-settings

-- `index_granularity = 0` is now valid when `index_granularity_bytes > 0`,
-- meaning granule size is driven purely by bytes with no row-count upper bound.

-- Both zero should still be rejected.
CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS index_granularity = 0, index_granularity_bytes = 0; -- { serverError BAD_ARGUMENTS }

-- `index_granularity = 0` with `index_granularity_bytes > 0` should work,
-- and granules should be byte-sized (not clamped to 1 row per granule).
-- Insert 10000 UInt64 rows (> old default of 8192) = 80000 bytes uncompressed.
-- With `index_granularity_bytes = 1000000` the writer computes granularity = 125000 rows,
-- fitting all 10000 rows into 1 granule → 1 mark.
DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS index_granularity = 0, index_granularity_bytes = 1000000,
             min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t SELECT number FROM numbers(10000);
OPTIMIZE TABLE t FINAL;
-- Should be 2 (1 data granule covering all rows, plus the final sentinel mark),
-- not 10001 (10000 data marks at 1 row each, plus final mark).
SELECT marks FROM system.parts
WHERE database = currentDatabase() AND table = 't' AND active;

-- Verify that SELECT actually works (roundRowsOrBytesToMarks must not divide by zero).
SELECT count() FROM t;
SELECT * FROM t FORMAT Null;

DROP TABLE t;
