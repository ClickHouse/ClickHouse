-- Tags: long
-- Exercise the parallel two-level DISTINCT build for nullable single-key,
-- multi-column fixed-key (`keys128`), and hashed-fallback paths. Lowering
-- `distinct_two_level_threshold` to 1000 forces promotion so the per-bucket
-- parallel build runs; results MUST match the serial path (threshold 0 disables
-- promotion).

SET max_threads = 8;

-- nullable single key (nullable_keys128 path)
SET distinct_two_level_threshold = 1000;
SELECT count() FROM (SELECT DISTINCT if(number % 7 = 0, NULL, number % 300000) AS k FROM numbers_mt(4000000));
SET distinct_two_level_threshold = 0;
SELECT count() FROM (SELECT DISTINCT if(number % 7 = 0, NULL, number % 300000) AS k FROM numbers_mt(4000000));

-- multi-column fixed key (keys128 path)
SET distinct_two_level_threshold = 1000;
SELECT count() FROM (SELECT DISTINCT number % 40000 AS a, (number % 50000)::UInt32 AS b FROM numbers_mt(4000000));
SET distinct_two_level_threshold = 0;
SELECT count() FROM (SELECT DISTINCT number % 40000 AS a, (number % 50000)::UInt32 AS b FROM numbers_mt(4000000));

-- hashed fallback (wide/complex key: UInt64 + String + UInt64)
SET distinct_two_level_threshold = 1000;
SELECT count() FROM (SELECT DISTINCT number % 30000 AS a, toString(number % 30000) AS b, (number % 20000)::UInt64 c FROM numbers_mt(4000000));
SET distinct_two_level_threshold = 0;
SELECT count() FROM (SELECT DISTINCT number % 30000 AS a, toString(number % 30000) AS b, (number % 20000)::UInt64 c FROM numbers_mt(4000000));

-- Cross-check: two-level result equals serial result for each path (emits 1 on match)
SELECT
(
    SELECT count() FROM (SELECT DISTINCT if(number % 7 = 0, NULL, number % 300000) AS k FROM numbers_mt(4000000)) SETTINGS distinct_two_level_threshold = 1000, max_threads = 8
) = (
    SELECT count() FROM (SELECT DISTINCT if(number % 7 = 0, NULL, number % 300000) AS k FROM numbers_mt(4000000)) SETTINGS distinct_two_level_threshold = 0
);

SELECT
(
    SELECT count() FROM (SELECT DISTINCT number % 40000 AS a, (number % 50000)::UInt32 AS b FROM numbers_mt(4000000)) SETTINGS distinct_two_level_threshold = 1000, max_threads = 8
) = (
    SELECT count() FROM (SELECT DISTINCT number % 40000 AS a, (number % 50000)::UInt32 AS b FROM numbers_mt(4000000)) SETTINGS distinct_two_level_threshold = 0
);

SELECT
(
    SELECT count() FROM (SELECT DISTINCT number % 30000 AS a, toString(number % 30000) AS b, (number % 20000)::UInt64 c FROM numbers_mt(4000000)) SETTINGS distinct_two_level_threshold = 1000, max_threads = 8
) = (
    SELECT count() FROM (SELECT DISTINCT number % 30000 AS a, toString(number % 30000) AS b, (number % 20000)::UInt64 c FROM numbers_mt(4000000)) SETTINGS distinct_two_level_threshold = 0
);
