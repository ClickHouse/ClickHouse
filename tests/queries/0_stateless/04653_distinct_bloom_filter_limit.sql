-- Regression test: the pre-DISTINCT `check_only` mode stops recording keys once the set grows past
-- the threshold, so it forwards every occurrence of a key seen for the first time after that point.
-- Such rows must not be counted towards `limit_hint`, otherwise `SELECT DISTINCT ... LIMIT N` stops
-- a stream after N repeats of a single key and loses the distinct values that come after them.

SET distinct_set_limit_for_enabling_bloom_filter = 1;
-- An unreachable pass ratio disables the bloom filter after the first chunk, so the plain hash set
-- grows past twice the limit above and the transform switches to `check_only` almost immediately.
SET distinct_pass_ratio_threshold_for_disabling_bloom_filter = 1;
SET max_threads = 4, max_block_size = 1024;

-- 10 distinct values at the beginning, then one value repeated 100000 times, then 1000 more
-- distinct values. Every LIMIT below is satisfiable many times over.
SELECT count() FROM (SELECT DISTINCT v FROM (SELECT if(number < 10, number, if(number < 100000, 1000000, number)) AS v FROM numbers_mt(101000)) LIMIT 5);
SELECT count() FROM (SELECT DISTINCT v FROM (SELECT if(number < 10, number, if(number < 100000, 1000000, number)) AS v FROM numbers_mt(101000)) LIMIT 100);
SELECT count() FROM (SELECT DISTINCT v FROM (SELECT if(number < 10, number, if(number < 100000, 1000000, number)) AS v FROM numbers_mt(101000)) LIMIT 500);

-- The full result set stays exact.
SELECT count(), uniqExact(v) FROM (SELECT DISTINCT v FROM (SELECT if(number < 10, number, if(number < 100000, 1000000, number)) AS v FROM numbers_mt(101000)));
