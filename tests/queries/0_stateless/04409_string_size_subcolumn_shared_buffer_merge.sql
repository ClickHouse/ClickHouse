-- Regression test for a heap-use-after-free / double-free when reading a String `.size`
-- subcolumn together with the full String column across many granules.
--
-- With string_serialization_version = 'with_size_stream' the sizes are stored in a separate
-- stream. The per-column deserialize state keeps the sizes column, and that same column is
-- also placed into the substreams cache (and can be handed out as the `.size` subcolumn
-- output). When a single read spans several granules (continue_reading, i.e. max_block_size
-- larger than index_granularity) the sizes were appended in place via `assumeMutable`, which
-- reallocated the shared buffer and left the cache / emitted column pointing at freed memory.
-- Two symmetric append sites had the bug: the full-string path in SerializationString and the
-- mirror `.size` path in SerializationStringSize (reached when the `.size` output column is
-- shared with SerializationString's persistent state). Under sanitizers this shows up as a
-- double-free in a background merge thread. Both sites now clone via `IColumn::mutate`.
--
-- The test forces those paths (tiny granule, wide part, reads spanning many granules, and a
-- read over several discontiguous mark ranges) and checks that the `.size` values are correct.
-- Without the fix it crashes under ASan.

DROP TABLE IF EXISTS t_string_size_shared;

CREATE TABLE t_string_size_shared (k UInt64, s String, big String)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 32,
         min_bytes_for_wide_part = 0,
         string_serialization_version = 'with_size_stream';

INSERT INTO t_string_size_shared
SELECT number, repeat('a', number % 80), repeat('x', number % 200)
FROM numbers(20000);

OPTIMIZE TABLE t_string_size_shared FINAL;

-- Read the `.size` subcolumn and the full string together, with a block much larger than the
-- granule so one read appends sizes across many granules (the triggering path).
-- `optimize_functions_to_subcolumns = 0` keeps `length(s)` reading the full String data stream
-- (the patched deserialize path) instead of being rewritten to the `s.size` subcolumn.
SELECT countIf(s.size != length(s)) AS bad_s,
       countIf(big.size != length(big)) AS bad_big,
       count() AS total
FROM t_string_size_shared
SETTINGS max_threads = 1, max_block_size = 65536, optimize_functions_to_subcolumns = 0;

-- Exercise the mirror path in SerializationStringSize: read the `.size` subcolumn AND the full
-- string over several discontiguous mark ranges (disjoint key intervals). Appending the second
-- and later ranges hits the shared-column case (`.size` output aliased by SerializationString's
-- persistent state) that must clone instead of appending in place.
SELECT countIf(s.size != length(s)) AS bad_s,
       countIf(big.size != length(big)) AS bad_big,
       count() AS total
FROM t_string_size_shared
WHERE k < 500 OR (k >= 5000 AND k < 5500) OR (k >= 15000 AND k < 15500)
SETTINGS max_threads = 1, max_block_size = 65536, optimize_functions_to_subcolumns = 0,
         merge_tree_min_rows_for_seek = 0, merge_tree_min_bytes_for_seek = 0;

-- Same, reading the `.size` subcolumns only (they are served from the shared sizes buffer).
SELECT sum(s.size) AS sum_s, sum(big.size) AS sum_big
FROM t_string_size_shared
SETTINGS max_threads = 1, max_block_size = 65536;

-- Per-granule block size must give identical results.
SELECT sum(s.size) AS sum_s, sum(big.size) AS sum_big
FROM t_string_size_shared
SETTINGS max_threads = 1, max_block_size = 32;

DROP TABLE t_string_size_shared;
