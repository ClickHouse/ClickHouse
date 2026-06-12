-- Tags: no-random-merge-tree-settings, no-parallel-replicas
-- no-random-merge-tree-settings: part layout and compression must be stable for the byte-count assertion.

-- When a query references a column whose result is fully constant-folded away
-- (e.g. isNullable(lc), which depends only on the type), ReadFromMergeTree has no
-- column to read and must pick one just to know the row count. It used to pick the
-- column with the smallest in-memory value size, which mis-ranks a compactly stored
-- LowCardinality(String) (tiny on disk) above a UInt64 (8 bytes in memory but a much
-- larger column on disk), so the reader read the large column. The carrier should be
-- the column that is cheapest to read from disk instead.

DROP TABLE IF EXISTS t_smallest_column;

-- ORDER BY tuple() so there is no sorting-key column: the carrier choice is then the
-- only thing that decides what is read. With a sorting key, the split-ranges read path
-- (merge_tree_read_split_ranges..._injection_probability) would additionally read the
-- key column and inflate the byte count regardless of the carrier.
-- Wide parts so per-column on-disk sizes are available to the planner.
CREATE TABLE t_smallest_column (x UInt64, lc LowCardinality(Nullable(String)))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- lc has very few distinct values -> a few KB on disk after LowCardinality + compression.
-- x is dense and unique -> several MB on disk.
INSERT INTO t_smallest_column SELECT number, toString(number % 10) FROM numbers(1000000);

SELECT isNullable(lc) FROM t_smallest_column FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- lc compresses to a few KB while x is ~4 MB on disk. The carrier must be lc, so the
-- bytes actually read off disk stay tiny; the regression read x (~4 MB).
SELECT ProfileEvents['ReadCompressedBytes'] < 1000000
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND query = 'SELECT isNullable(lc) FROM t_smallest_column FORMAT Null;'
  AND current_database = currentDatabase()
  AND type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 1;

DROP TABLE t_smallest_column;
