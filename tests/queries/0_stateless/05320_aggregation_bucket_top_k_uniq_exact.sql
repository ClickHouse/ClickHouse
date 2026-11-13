-- The bucket top-K plan optimization ranks the groups of each two-level bucket by a count it reads from their states
-- without converting them: a lone `count()`, or the exact distinct count of `uniqExact`, which `COUNT(DISTINCT)`
-- resolves to, also as `uniqExactIf`. The first cells check the plan flag in `EXPLAIN actions = 1`; the estimate of
-- `uniq` stays on the ordinary conversion.
--
-- The other cells run the classic two-level conversion and print the top. Group k of the source has 100 rows with
-- j = 0..99 and the distinct values j % (k + 1) for k < 10, j % (k - 19900) for k >= 19990, and j % 50 otherwise, so
-- the ten largest and the ten smallest distinct counts are unique, and every bucket holds about 78 groups: the heap of
-- a bucket fills, and the selection destroys the states of the groups it rejects or evicts. The tables must become
-- two-level to have buckets: the source is read by several streams, and the keys are `UInt64` and `String`, whose
-- conversion functions are kept rather than reduced to their `UInt16` argument, which would get a fixed single-level
-- table. The cells cover the distinct count alone, next to other aggregates, with a Nullable argument, with a
-- condition, with two arguments, an ascending order, an offset, and a String key.

SET query_plan_enable_optimizations = 1, query_plan_push_down_limit = 1, query_plan_aggregation_bucket_top_k = 1;
SET count_distinct_implementation = 'uniqExact';
SET enable_adaptive_aggregator = 0, max_threads = 4, group_by_two_level_threshold = 100, group_by_two_level_threshold_bytes = 0;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET optimize_injective_functions_in_group_by = 0;

SELECT count() FROM (EXPLAIN actions = 1 SELECT number % 1000 AS k, uniqExact(number) AS u FROM numbers(100000) GROUP BY k ORDER BY u DESC LIMIT 10)
WHERE explain LIKE '%Bucket top-K: 10 descending%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT number % 1000 AS k, count(DISTINCT number) AS u FROM numbers(100000) GROUP BY k ORDER BY u DESC LIMIT 10)
WHERE explain LIKE '%Bucket top-K: 10 descending%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT number % 1000 AS k, uniqExactIf(number, number % 3 = 0) AS u FROM numbers(100000) GROUP BY k ORDER BY u DESC LIMIT 10)
WHERE explain LIKE '%Bucket top-K: 10 descending%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT number % 1000 AS k, uniq(number) AS u FROM numbers(100000) GROUP BY k ORDER BY u DESC LIMIT 10)
WHERE explain LIKE '%Bucket top-K%';

SELECT 'distinct count', arraySort(groupArray((u, k))) FROM (
    WITH intDiv(number, 20000) AS j
    SELECT toUInt64(number % 20000) AS k,
        uniqExact(multiIf(k < 10, j % (k + 1), k >= 19990, j % (k - 19900), j % 50)) AS u
    FROM numbers_mt(2000000) GROUP BY k ORDER BY u DESC LIMIT 10);

SELECT 'COUNT(DISTINCT)', arraySort(groupArray((u, k))) FROM (
    WITH intDiv(number, 20000) AS j
    SELECT toUInt64(number % 20000) AS k,
        count(DISTINCT multiIf(k < 10, j % (k + 1), k >= 19990, j % (k - 19900), j % 50)) AS u
    FROM numbers_mt(2000000) GROUP BY k ORDER BY u DESC LIMIT 10);

SELECT 'with other aggregates', arraySort(groupArray((u, k, c, s))) FROM (
    WITH intDiv(number, 20000) AS j
    SELECT toUInt64(number % 20000) AS k, count() AS c, sum(j) AS s,
        uniqExact(multiIf(k < 10, j % (k + 1), k >= 19990, j % (k - 19900), j % 50)) AS u
    FROM numbers_mt(2000000) GROUP BY k ORDER BY u DESC LIMIT 10);

SELECT 'Nullable argument', arraySort(groupArray((u, k))) FROM (
    WITH intDiv(number, 20000) AS j
    SELECT toUInt64(number % 20000) AS k,
        uniqExact(nullIf(multiIf(k < 10, j % (k + 1), k >= 19990, j % (k - 19900), j % 50), 0)) AS u
    FROM numbers_mt(2000000) GROUP BY k ORDER BY u DESC LIMIT 10);

SELECT 'uniqExactIf', arraySort(groupArray((u, k))) FROM (
    WITH intDiv(number, 20000) AS j
    SELECT toUInt64(number % 20000) AS k,
        uniqExactIf(multiIf(k < 10, j % (k + 1), k >= 19990, j % (k - 19900), j % 50), j % 10 != 9) AS u
    FROM numbers_mt(2000000) GROUP BY k ORDER BY u DESC LIMIT 10);

SELECT 'two arguments', arraySort(groupArray((u, k))) FROM (
    WITH intDiv(number, 20000) AS j
    SELECT toUInt64(number % 20000) AS k,
        uniqExact(k % 7, multiIf(k < 10, j % (k + 1), k >= 19990, j % (k - 19900), j % 50)) AS u
    FROM numbers_mt(2000000) GROUP BY k ORDER BY u DESC LIMIT 10);

SELECT 'ascending', arraySort(groupArray((u, k))) FROM (
    WITH intDiv(number, 20000) AS j
    SELECT toUInt64(number % 20000) AS k,
        uniqExact(multiIf(k < 10, j % (k + 1), k >= 19990, j % (k - 19900), j % 50)) AS u
    FROM numbers_mt(2000000) GROUP BY k ORDER BY u ASC LIMIT 10);

SELECT 'offset', arraySort(groupArray((u, k))) FROM (
    WITH intDiv(number, 20000) AS j
    SELECT toUInt64(number % 20000) AS k,
        uniqExact(multiIf(k < 10, j % (k + 1), k >= 19990, j % (k - 19900), j % 50)) AS u
    FROM numbers_mt(2000000) GROUP BY k ORDER BY u DESC LIMIT 5 OFFSET 3);

SELECT 'String key', arraySort(groupArray((u, k))) FROM (
    WITH intDiv(number, 20000) AS j
    SELECT toString(number % 20000) AS k,
        uniqExact(multiIf(number % 20000 < 10, j % (number % 20000 + 1), number % 20000 >= 19990, j % (number % 20000 - 19900), j % 50)) AS u
    FROM numbers_mt(2000000) GROUP BY k ORDER BY u DESC LIMIT 10);
