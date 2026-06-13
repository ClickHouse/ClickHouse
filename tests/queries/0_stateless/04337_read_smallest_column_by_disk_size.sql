-- Tags: no-random-merge-tree-settings, no-parallel-replicas
-- no-random-merge-tree-settings: part layout and compression must be stable for the byte-count assertion.

-- When a query references a column whose result is fully constant-folded away
-- (e.g. isNullable(lc), which depends only on the type), ReadFromMergeTree has no
-- column to read and must pick one just to know the row count. It used to pick the
-- column with the smallest in-memory value size, which mis-ranks a compactly stored
-- LowCardinality(String) (tiny on disk) above a UInt64 (8 bytes in memory but a much
-- larger column on disk), so the reader read the large column. The carrier should be
-- the column that is cheapest to read from disk instead.

-- The carrier path is only reached when the unused column input is pruned from the plan. With
-- query_plan_remove_unused_columns disabled (CI randomizes it), the planner keeps an arbitrary
-- column as an input and ReadFromMergeTree reads that column directly instead of choosing a
-- carrier, which would read megabytes and break the byte-count assertions. Pin it on.
SET query_plan_remove_unused_columns = 1;

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

-- Compact parts keep every column in one file and report a per-column on-disk size of 0, so the
-- planner cannot rank them by on-disk size. It must then fall back to the in-memory heuristic
-- rather than blindly taking the first column: here 'big' (a dense String) is first but 'tiny'
-- (a UInt8) is cheap to read. The regression picked 'big' and read the whole String stream.
DROP TABLE IF EXISTS t_smallest_column_compact;

CREATE TABLE t_smallest_column_compact (big String, tiny UInt8)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

INSERT INTO t_smallest_column_compact SELECT repeat('x', 100), 0 FROM numbers(50000);

SELECT isNullable(tiny) FROM t_smallest_column_compact FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- 'tiny' reads a few hundred bytes; 'big' reads ~20 KB. The carrier must be 'tiny'.
SELECT ProfileEvents['ReadCompressedBytes'] < 5000
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND query = 'SELECT isNullable(tiny) FROM t_smallest_column_compact FORMAT Null;'
  AND current_database = currentDatabase()
  AND type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 1;

DROP TABLE t_smallest_column_compact;

-- Mixed compact/wide layout. The aggregate on-disk size is only trustworthy when every selected
-- part reports a positive per-column size: a compact part keeps all columns in one file and reports
-- 0 for each, so it contributes nothing to the sum while its real read cost may dominate the table.
-- Here a small WIDE part makes 'big' (a constant string, ~1 KB) look smaller than 'tiny' (random
-- UInt8, ~2 KB), but the dominant COMPACT part stores 'big' as long random strings (~60 MB) and
-- 'tiny' as a constant (~3 KB). Summing only the wide part's positive sizes would pick 'big' and
-- read ~60 MB off the compact part. The planner must distrust the aggregate when any part reports 0
-- and fall back to the in-memory heuristic, which correctly ranks the UInt8 'tiny' below 'big'.
DROP TABLE IF EXISTS t_smallest_column_mixed;

CREATE TABLE t_smallest_column_mixed (big String, tiny UInt8)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- Small WIDE part: big constant (cheap on disk), tiny random (less cheap). big looks smallest here.
INSERT INTO t_smallest_column_mixed SELECT repeat('x', 100), rand() FROM numbers(2000);
-- Flip thresholds so the next, much larger insert is a COMPACT part (per-column size reported as 0).
ALTER TABLE t_smallest_column_mixed MODIFY SETTING min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
-- Dominant COMPACT part: big is long random strings (expensive), tiny is constant (cheap).
INSERT INTO t_smallest_column_mixed SELECT randomString(200), 0 FROM numbers(300000);

SELECT isNullable(tiny) FROM t_smallest_column_mixed FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- Carrier must be 'tiny': reading 'big' off the compact part costs ~60 MB, 'tiny' costs a few KB.
SELECT ProfileEvents['ReadCompressedBytes'] < 100000
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND query = 'SELECT isNullable(tiny) FROM t_smallest_column_mixed FORMAT Null;'
  AND current_database = currentDatabase()
  AND type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 1;

DROP TABLE t_smallest_column_mixed;

-- The carrier must be ranked over the parts that actually survive to the read. An aggregate
-- projection that covers only some parts serves count() from the projection for the covered parts
-- and leaves the uncovered parent parts to a no-columns carrier read, after filterPartsByProjection
-- has dropped the covered parts. Ranking the carrier before that (over all parts) lets a column
-- that is cheap only in a covered, pruned part win, so the surviving part is read through an
-- expensive column. Here the surviving part stores 'a' as random strings (~15 MB) and 'b' as a
-- constant (cheap), while the projection-covered part stores 'b' as random strings (~100 MB) and
-- 'a' as a constant. The global aggregate makes 'a' look cheapest and reads ~15 MB off the survivor;
-- ranking over the surviving part picks 'b' and keeps the read tiny.
DROP TABLE IF EXISTS t_smallest_column_projection;

CREATE TABLE t_smallest_column_projection (a String, b String)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- Surviving parent part (inserted before the projection exists): 'a' expensive, 'b' cheap.
INSERT INTO t_smallest_column_projection SELECT randomString(50), 'c' FROM numbers(300000);
ALTER TABLE t_smallest_column_projection ADD PROJECTION pc (SELECT count());
-- Projection-covered part (dropped by filterPartsByProjection): 'a' cheap, 'b' dominant/expensive.
INSERT INTO t_smallest_column_projection SELECT 'c', randomString(200) FROM numbers(500000);

-- count() is served by the projection for the covered part and by a carrier read for the survivor.
-- optimize_trivial_count_query/implicit_projections off so it takes the real no-columns read path.
-- FORMAT Null: only the byte count below is asserted; the row count itself is not the subject here.
SELECT count() FROM t_smallest_column_projection
SETTINGS optimize_use_projections = 1, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- Carrier must be 'b' (cheap in the survivor): reads a few KB. The regression ranked over all parts,
-- picked 'a' and read ~15 MB off the surviving part.
SELECT ProfileEvents['ReadCompressedBytes'] < 1000000
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND query LIKE '%count() FROM t_smallest_column_projection%'
  AND query NOT LIKE '%query_log%'
  AND current_database = currentDatabase()
  AND type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 1;

DROP TABLE t_smallest_column_projection;

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

-- Row count comes from part metadata, so it stays available even though the part is unreadable.
-- Pin optimize_trivial_count_query: with it off (and implicit projections off) count() would take
-- the no-columns read path, hit the unreadable part and throw NO_SUCH_COLUMN_IN_TABLE instead.
SELECT count() FROM t_evolved_pruned SETTINGS optimize_trivial_count_query = 1;

DROP TABLE t_evolved_pruned;
