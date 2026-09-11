-- Regression test for reading an Array `.size0` subcolumn together with the full Array column
-- across many granules of a compact part.
--
-- Array sizes/offsets live in a separate stream (Substream::ArraySizes). When one read spans
-- several granules (max_block_size much larger than index_granularity), the offsets are appended
-- to the reader's per-column result across calls. `MergeTreeReaderCompact::readData` has two
-- subcolumn branches: with per-substream marks it reads the subcolumn through its serialization,
-- and without them it reads the full column and extracts the subcolumn in memory. The second
-- branch is the one that appends into a possibly shared column, and it is only reachable for
-- parts written with `write_marks_for_substreams_in_compact_parts = 0`, so the tables below pin
-- that setting to cover both branches.
--
-- `optimize_functions_to_subcolumns = 0` keeps `length(a)` reading the full array data stream
-- instead of being rewritten to `a.size0`, so the full column and the subcolumn are read together.

DROP TABLE IF EXISTS t_array_size_shared;
DROP TABLE IF EXISTS t_array_size_shared_marks;

-- No per-substream marks: exercises the read-full-column-then-extract-subcolumn branch.
CREATE TABLE t_array_size_shared (k UInt64, a Array(UInt32), big Array(UInt32))
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 32, index_granularity_bytes = '100Gi', min_bytes_for_wide_part = 1000000000,
         write_marks_for_substreams_in_compact_parts = 0;

-- Per-substream marks (the current default): exercises the subcolumn-serialization branch.
CREATE TABLE t_array_size_shared_marks (k UInt64, a Array(UInt32), big Array(UInt32))
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 32, index_granularity_bytes = '100Gi', min_bytes_for_wide_part = 1000000000,
         write_marks_for_substreams_in_compact_parts = 1;

INSERT INTO t_array_size_shared
SELECT number, range(number % 80), range(number % 200)
FROM numbers(20000);

INSERT INTO t_array_size_shared_marks
SELECT number, range(number % 80), range(number % 200)
FROM numbers(20000);

OPTIMIZE TABLE t_array_size_shared FINAL;
OPTIMIZE TABLE t_array_size_shared_marks FINAL;

-- Preconditions: both tables must hold Compact parts with several fixed-size granules, so a
-- read with a large block really spans granules. index_granularity_bytes is pinned because the
-- test runner randomizes it and a small value makes granule sizes vary; it is pinned to a large
-- value rather than 0 because 0 turns off adaptive granularity, which would force Wide parts.
SELECT table, part_type FROM system.parts
WHERE database = currentDatabase()
  AND table IN ('t_array_size_shared', 't_array_size_shared_marks')
  AND active
ORDER BY table;

-- Assert the effective mark layout, not just the recorded setting: the reader picks its branch
-- from the part's mark type, so a mark for the `a.size0` substream must be absent in the first
-- table and present in the second. Otherwise the pins could silently stop taking effect and both
-- tables would exercise the same branch while every result assertion below still passed.
SELECT 't_array_size_shared' AS table,
       countIf(rows_in_granule > 0) > 1 AS many_granules,
       minIf(rows_in_granule, rows_in_granule > 0) = 32
           AND max(rows_in_granule) = 32 AS granule_rows_pinned,
       uniqExact(isNotNull((`a.size0.mark`).offset_in_compressed_file)) = 1
           AND max(isNotNull((`a.size0.mark`).offset_in_compressed_file)) AS has_substream_mark
FROM mergeTreeIndex(currentDatabase(), t_array_size_shared, with_marks = true);

SELECT 't_array_size_shared_marks' AS table,
       countIf(rows_in_granule > 0) > 1 AS many_granules,
       minIf(rows_in_granule, rows_in_granule > 0) = 32
           AND max(rows_in_granule) = 32 AS granule_rows_pinned,
       uniqExact(isNotNull((`a.size0.mark`).offset_in_compressed_file)) = 1
           AND max(isNotNull((`a.size0.mark`).offset_in_compressed_file)) AS has_substream_mark
FROM mergeTreeIndex(currentDatabase(), t_array_size_shared_marks, with_marks = true);

-- Read the `.size0` subcolumn and the full array together, with a block much larger than the
-- granule so one read appends offsets across many granules.
SELECT countIf(a.size0 != length(a)) AS bad_a,
       countIf(big.size0 != length(big)) AS bad_big,
       count() AS total
FROM t_array_size_shared
SETTINGS max_threads = 1, max_block_size = 65536, merge_tree_min_rows_for_concurrent_read = 0,
         preferred_block_size_bytes = 0, optimize_functions_to_subcolumns = 0;

SELECT countIf(a.size0 != length(a)) AS bad_a,
       countIf(big.size0 != length(big)) AS bad_big,
       count() AS total
FROM t_array_size_shared_marks
SETTINGS max_threads = 1, max_block_size = 65536, merge_tree_min_rows_for_concurrent_read = 0,
         preferred_block_size_bytes = 0, optimize_functions_to_subcolumns = 0;

-- Same, reading the `.size0` subcolumns only.
SELECT sum(a.size0) AS sum_a, sum(big.size0) AS sum_big
FROM t_array_size_shared
SETTINGS max_threads = 1, max_block_size = 65536, preferred_block_size_bytes = 0;

SELECT sum(a.size0) AS sum_a, sum(big.size0) AS sum_big
FROM t_array_size_shared_marks
SETTINGS max_threads = 1, max_block_size = 65536, preferred_block_size_bytes = 0;

-- Per-granule block size must give identical results.
SELECT sum(a.size0) AS sum_a, sum(big.size0) AS sum_big
FROM t_array_size_shared
SETTINGS max_threads = 1, max_block_size = 32;

SELECT sum(a.size0) AS sum_a, sum(big.size0) AS sum_big
FROM t_array_size_shared_marks
SETTINGS max_threads = 1, max_block_size = 32;

DROP TABLE t_array_size_shared;
DROP TABLE t_array_size_shared_marks;
