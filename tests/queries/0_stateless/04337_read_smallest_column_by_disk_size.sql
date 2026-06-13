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

-- The carrier is global to the read, so it must be picked from the aggregate on-disk size over
-- all selected parts, not sampled from one part. Here x is tiny in the first part (all zeros) but
-- dense in the much larger later parts, while lc is cheap everywhere. Sampling the first part would
-- pick x and read several MB; the aggregate must pick lc and keep the read tiny.
DROP TABLE IF EXISTS t_smallest_column_multipart;

CREATE TABLE t_smallest_column_multipart (x UInt64, lc LowCardinality(Nullable(String)))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_smallest_column_multipart SELECT 0, toString(number % 10) FROM numbers(100000);
INSERT INTO t_smallest_column_multipart SELECT number, toString(number % 10) FROM numbers(2000000);

SELECT isNullable(lc) FROM t_smallest_column_multipart FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['ReadCompressedBytes'] < 1000000
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND query = 'SELECT isNullable(lc) FROM t_smallest_column_multipart FORMAT Null;'
  AND current_database = currentDatabase()
  AND type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 1;

DROP TABLE t_smallest_column_multipart;

-- The carrier-column lookup must not throw LOGICAL_ERROR on a selected part that has files
-- for none of the current physical columns (it predates the current schema). Such a part is
-- built by detaching it, evolving the schema so all of its columns are dropped, then
-- re-attaching. The all-pruned read must degrade gracefully (the part is unreadable, like any
-- column read on it) instead of crashing the server.
DROP TABLE IF EXISTS t_evolved_pruned;

CREATE TABLE t_evolved_pruned (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_evolved_pruned VALUES (1, 10), (2, 20), (3, 30);

ALTER TABLE t_evolved_pruned DETACH PART 'all_1_1_0';
ALTER TABLE t_evolved_pruned ADD COLUMN c UInt64 DEFAULT 0;
ALTER TABLE t_evolved_pruned DROP COLUMN a;
ALTER TABLE t_evolved_pruned DROP COLUMN b;
ALTER TABLE t_evolved_pruned ATTACH PART 'all_1_1_0';

SELECT 1 FROM t_evolved_pruned FORMAT Null; -- { serverError NO_SUCH_COLUMN_IN_TABLE }

SELECT count() FROM t_evolved_pruned;

DROP TABLE t_evolved_pruned;
