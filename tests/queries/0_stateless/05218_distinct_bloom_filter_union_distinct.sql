-- `UNION DISTINCT` builds its preliminary `DistinctStep` on a separate code path from plain
-- `SELECT DISTINCT`, so the Bloom-filter settings must be forwarded there as well. The filter is
-- only allocated in the preliminary step, and its size is accounted for by `max_bytes_in_distinct`,
-- so a filter that exceeds the byte limit on its own proves that the preliminary step actually
-- reaches the Bloom-filter mode.

-- The Bloom filter is only used when no exact row limit is configured, and the test profile of the
-- CI sets `max_rows_in_distinct` to a large value, so it has to be reset explicitly here.
SET max_rows_in_distinct = 0;
SET distinct_set_limit_for_enabling_bloom_filter = 100;
SET distinct_pass_ratio_threshold_for_disabling_bloom_filter = 0.1;
SET allow_preliminary_distinct_abandoning = 0;
SET max_threads = 4, max_block_size = 128;
SET max_bytes_in_distinct = 200000;

-- The hash sets alone stay below the limit: with a small filter the query goes through and stays exact.
SET distinct_bloom_filter_bytes = 4096;
SELECT count() FROM (SELECT number FROM numbers_mt(4000) UNION DISTINCT SELECT number + 2000 FROM numbers_mt(4000));

-- A 1 MiB filter is over the limit on its own, so the very same query must be rejected.
SET distinct_bloom_filter_bytes = 1048576;
SELECT count() FROM (SELECT number FROM numbers_mt(4000) UNION DISTINCT SELECT number + 2000 FROM numbers_mt(4000)); -- { serverError SET_SIZE_LIMIT_EXCEEDED }

