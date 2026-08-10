-- The bloom filter of the pre-DISTINCT step holds keys that would otherwise live in the hash set,
-- so its allocation must be accounted for by `max_bytes_in_distinct` as well. Otherwise a query
-- could keep an arbitrarily large filter without the byte limit ever firing.

SET distinct_set_limit_for_enabling_bloom_filter = 100;
SET distinct_pass_ratio_threshold_for_disabling_bloom_filter = 0.1;
SET max_threads = 4, max_block_size = 1024;
SET max_bytes_in_distinct = 200000;

-- The hash sets alone stay below the limit: with a small filter the query goes through.
SET distinct_bloom_filter_bytes = 4096;
SELECT count() FROM (SELECT DISTINCT number FROM numbers_mt(4000));

-- A 1 MiB filter is over the limit on its own, so the very same query must be rejected.
SET distinct_bloom_filter_bytes = 1048576;
SELECT count() FROM (SELECT DISTINCT number FROM numbers_mt(4000)); -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- With `break` the query returns a partial result instead of throwing.
SET distinct_overflow_mode = 'break';
SELECT count() BETWEEN 1 AND 4000 FROM (SELECT DISTINCT number FROM numbers_mt(4000));
