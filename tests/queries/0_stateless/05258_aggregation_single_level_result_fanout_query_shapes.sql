-- A single-level GROUP BY result smaller than max_block_size is split into several chunks when the output fans out.
-- A randomized max_block_size below 20000, or a two-level result, would split it on its own.
SET max_threads = 4, max_block_size = 65409, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0;

-- count() used by the outer query; an unused one is dropped from the aggregation.
SELECT max(bs) < 20000 AND sum(c) = 200000
FROM (SELECT blockSize() AS bs, c FROM (SELECT number % 20000 AS k, count() AS c FROM numbers(200000) GROUP BY k));

-- No aggregate functions, UInt64 key.
SELECT max(bs) < 20000
FROM (SELECT blockSize() AS bs FROM (SELECT intDiv(number, 10) AS k FROM numbers(200000) GROUP BY k));

-- GROUPING SETS over one stream.
SELECT max(bs) < 20000
FROM (SELECT blockSize() AS bs FROM (SELECT number % 20000 AS k, number % 3 AS g FROM numbers(200000) GROUP BY GROUPING SETS ((k), (g))));
