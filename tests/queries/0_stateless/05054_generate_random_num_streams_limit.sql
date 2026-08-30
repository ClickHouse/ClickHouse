-- `GenerateRandom` creates one source per requested stream, and only a trivial `LIMIT` bounds that
-- count. An absurd `max_streams_to_max_threads_ratio` must be rejected with `PARAMETER_OUT_OF_BOUND`
-- instead of throwing `std::length_error` from `pipes.reserve`, which aborts the server in debug and
-- sanitizer builds. A count the reduction brings back under the limit must still be served.

-- Keep effective `max_threads` as set below. Under memory pressure (e.g. per_test_coverage)
-- `getMaxThreadsForAvailableMemory` clamps `max_threads` down to 1, which stops the ratio from being
-- applied, so a buggy build would false-pass.
SET max_threads_min_free_memory_per_thread = 0;

-- `DISTINCT` defeats the trivial-`LIMIT` optimization, so the whole requested count reaches the read.
SELECT DISTINCT s FROM generateRandom('s FixedString(37)', 1, 25, 2) LIMIT 3
SETTINGS max_threads = 4, max_streams_to_max_threads_ratio = 288230376151711744; -- { serverError PARAMETER_OUT_OF_BOUND }

-- A `LIMIT` equal to `max_block_size` keeps `max_streams` above 1, and the reduction's
-- `num_streams * max_block_size` wraps, so the reduction does not apply.
SELECT n FROM generateRandom('n UInt8') LIMIT 16
SETTINGS max_block_size = 16, preferred_block_size_bytes = 0, max_threads = 4,
    max_streams_to_max_threads_ratio = 288230376151711744; -- { serverError PARAMETER_OUT_OF_BOUND }

-- The same request without the downstream resize.
SELECT n FROM generateRandom('n UInt8') LIMIT 16
SETTINGS max_block_size = 16, preferred_block_size_bytes = 0, max_threads = 4,
    parallelize_output_from_storages = 0, max_streams_to_max_threads_ratio = 288230376151711744; -- { serverError PARAMETER_OUT_OF_BOUND }

-- The reduction brings the same ratio down to a single source, so this read must be served. It stops
-- being served if the count is checked before the reduction.
SELECT count() FROM (SELECT n FROM generateRandom('n UInt8') LIMIT 1)
SETTINGS optimize_trivial_count_query = 0, max_block_size = 1, preferred_block_size_bytes = 0,
    max_threads = 4, parallelize_output_from_storages = 0,
    max_streams_to_max_threads_ratio = 288230376151711744;

-- An ordinary trivial `LIMIT` sets `max_streams` to 1 before the ratio applies.
SELECT count() FROM (SELECT s FROM generateRandom('s String') LIMIT 3)
SETTINGS optimize_trivial_count_query = 0, max_threads = 4,
    max_streams_to_max_threads_ratio = 288230376151711744;

-- Ordinary reads keep working with and without a ratio.
SELECT count() FROM (SELECT s FROM generateRandom('s String') LIMIT 1000)
SETTINGS optimize_trivial_count_query = 0, max_threads = 4;

SELECT count() FROM (SELECT s FROM generateRandom('s String') LIMIT 1000)
SETTINGS optimize_trivial_count_query = 0, max_threads = 4, max_streams_to_max_threads_ratio = 4;

-- A reduction to a few sources followed by the downstream resize back up to `max_threads`.
SELECT count() FROM (SELECT s FROM generateRandom('s String') LIMIT 100000)
SETTINGS optimize_trivial_count_query = 0, max_block_size = 65536, max_threads = 4,
    max_streams_to_max_threads_ratio = 4;
