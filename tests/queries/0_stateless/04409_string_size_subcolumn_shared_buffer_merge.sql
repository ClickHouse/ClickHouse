-- Regression test for a heap-use-after-free / double-free when reading a String `.size`
-- subcolumn together with the full String column across many granules.
--
-- With string_serialization_version = 'with_size_stream' the sizes are stored in a separate
-- stream. The per-column deserialize state keeps the sizes column, and that same column is
-- also placed into the substreams cache (and can be handed out as the `.size` subcolumn
-- output). When a single read spans several granules (continue_reading, i.e. max_block_size
-- larger than index_granularity) the sizes were appended in place via `assumeMutable()`,
-- which reallocated the shared buffer and left the cache / emitted column pointing at freed
-- memory. Under sanitizers this shows up as a double-free in a background merge thread.
--
-- The test forces that path (tiny granule, wide part, a read spanning many granules) and
-- checks that the `.size` values are correct. Without the fix it crashes under ASan.

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
SELECT countIf(s.size != length(s)) AS bad_s,
       countIf(big.size != length(big)) AS bad_big,
       count() AS total
FROM t_string_size_shared
SETTINGS max_threads = 1, max_block_size = 65536;

-- Same, reading the `.size` subcolumns only (they are served from the shared sizes buffer).
SELECT sum(s.size) AS sum_s, sum(big.size) AS sum_big
FROM t_string_size_shared
SETTINGS max_threads = 1, max_block_size = 65536;

-- Per-granule block size must give identical results.
SELECT sum(s.size) AS sum_s, sum(big.size) AS sum_big
FROM t_string_size_shared
SETTINGS max_threads = 1, max_block_size = 32;

DROP TABLE t_string_size_shared;
