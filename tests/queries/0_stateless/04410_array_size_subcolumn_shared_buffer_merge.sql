-- Regression test for a heap-use-after-free / SIGSEGV when reading an Array `.size0`
-- subcolumn together with the full Array column across many granules of a compact part.
--
-- The array sizes/offsets are stored in a separate stream (Substream::ArraySizes). When a
-- single read spans several granules (max_block_size much larger than index_granularity),
-- the offsets were appended in place while the offsets column could still be shared with the
-- reader's per-column result (the `.size0` output). In a background merge / compact-part read
-- this reallocated a buffer another holder still pointed at, surfacing as a SIGSEGV inside the
-- offsets resize (SerializationArray::deserializeOffsetsBinaryBulk ->
-- deserializeArraySizesPositionIndependent) under sanitizers.
--
-- This is the Array-offsets sibling of the String `.size` case in test
-- 04409_string_size_subcolumn_shared_buffer_merge. The read/build path was made COW-safe in
-- the MergeTree compact reader; this test guards the Array variant against regressions.

DROP TABLE IF EXISTS t_array_size_shared;

CREATE TABLE t_array_size_shared (k UInt64, a Array(UInt32), big Array(UInt32))
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 32, min_bytes_for_wide_part = 1000000000;

INSERT INTO t_array_size_shared
SELECT number, range(number % 80), range(number % 200)
FROM numbers(20000);

OPTIMIZE TABLE t_array_size_shared FINAL;

-- Read the `.size0` subcolumn and the full array together, with a block much larger than the
-- granule so one read appends offsets across many granules (the triggering path).
-- `optimize_functions_to_subcolumns = 0` keeps `length(a)` reading the full array data stream
-- (the patched deserialize path) instead of being rewritten to the `a.size0` subcolumn.
SELECT countIf(a.size0 != length(a)) AS bad_a,
       countIf(big.size0 != length(big)) AS bad_big,
       count() AS total
FROM t_array_size_shared
SETTINGS max_threads = 1, max_block_size = 65536, optimize_functions_to_subcolumns = 0;

-- Same, reading the `.size0` subcolumns only (served from the shared sizes buffer).
SELECT sum(a.size0) AS sum_a, sum(big.size0) AS sum_big
FROM t_array_size_shared
SETTINGS max_threads = 1, max_block_size = 65536;

-- Per-granule block size must give identical results.
SELECT sum(a.size0) AS sum_a, sum(big.size0) AS sum_big
FROM t_array_size_shared
SETTINGS max_threads = 1, max_block_size = 32;

DROP TABLE t_array_size_shared;
