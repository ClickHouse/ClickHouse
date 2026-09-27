-- Tags: long
SET max_threads = 8;
SET distinct_two_level_threshold = 1000; -- lowered to force two-level promotion
SELECT count() FROM (SELECT DISTINCT number % 500000 AS k FROM numbers_mt(4000000));
SELECT count() FROM (SELECT DISTINCT number % 500000 AS k FROM numbers_mt(4000000));
SELECT count() FROM (SELECT DISTINCT (number % 300000)::UInt32 AS k FROM numbers_mt(4000000));
SELECT count() FROM (SELECT DISTINCT toString(number % 400000) AS k FROM numbers_mt(4000000)); -- string path (key_string two-level, see 04327 for the serial cross-check)
