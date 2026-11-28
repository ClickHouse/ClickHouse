-- The bloom-filter and `check_only` pre-DISTINCT paths intentionally do not retain every key.
-- Disable them when `max_rows_in_distinct` is set, so row-limit accounting stays exact.
SET max_threads = 4, max_block_size = 1;
SET distinct_set_limit_for_enabling_bloom_filter = 1;
SET distinct_bloom_filter_bytes = 4096;
SET max_rows_in_distinct = 2;

-- The third value is already in the set when the Bloom filter would otherwise activate.
-- There are exactly two distinct values, so this must not exceed the row limit.
SELECT count() FROM (SELECT DISTINCT number % 2 FROM numbers_mt(3));

-- A third distinct value must still exceed the limit.
SELECT count() FROM (SELECT DISTINCT number FROM numbers_mt(3)); -- { serverError SET_SIZE_LIMIT_EXCEEDED }
