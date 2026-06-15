-- A `LIMIT` close to the `size_t` range used to make the top-K heap reserve
-- `1.5 * limit` rows up front, which the allocator rejected with "Too large
-- size passed to allocator". Such a limit keeps every group, so the optimized
-- result must match the unoptimized one.

SET max_threads = 1;
-- The CI test profile sets max_rows_to_group_by, which disables the optimization; reset it.
SET max_rows_to_group_by = 0;
SET optimize_trivial_group_by_limit_query = 0;

SELECT 'Single numeric key, huge limit';
SELECT k FROM (SELECT number % 10 AS k FROM numbers(1000)) GROUP BY k ORDER BY k LIMIT 9223372036854775806 SETTINGS enable_group_by_top_k_optimization = 1;

SELECT 'Single numeric key, huge limit with huge offset';
SELECT k FROM (SELECT number % 10 AS k FROM numbers(1000)) GROUP BY k ORDER BY k LIMIT 9223372036854775806, 10 SETTINGS enable_group_by_top_k_optimization = 1;

SELECT 'Composite key, huge limit';
SELECT a, b FROM (SELECT number % 5 AS a, number % 3 AS b FROM numbers(1000)) GROUP BY a, b ORDER BY a, b LIMIT 9223372036854775806 SETTINGS enable_group_by_top_k_optimization = 1;

SELECT 'String / serialized key, huge limit';
SELECT k FROM (SELECT toString(number % 10) AS k FROM numbers(1000)) GROUP BY k ORDER BY k LIMIT 9223372036854775806 SETTINGS enable_group_by_top_k_optimization = 1;

SELECT 'Pattern 2 (no ORDER BY), huge limit keeps all groups';
SELECT count() FROM (SELECT k FROM (SELECT number % 10 AS k FROM numbers(1000)) GROUP BY k LIMIT 9223372036854775806 SETTINGS enable_group_by_top_k_optimization = 1);

SELECT 'Result matches the unoptimized aggregation';
SELECT count(), countIf(complete) FROM
(
    SELECT l.s = f.s AS complete
    FROM (SELECT k, sum(v) AS s FROM (SELECT toUInt32(999 - (number % 1000)) AS k, 1 AS v FROM numbers(4000)) GROUP BY k ORDER BY k LIMIT 9223372036854775806 SETTINGS enable_group_by_top_k_optimization = 1) AS l
    INNER JOIN (SELECT k, sum(v) AS s FROM (SELECT toUInt32(999 - (number % 1000)) AS k, 1 AS v FROM numbers(4000)) GROUP BY k SETTINGS enable_group_by_top_k_optimization = 0) AS f USING (k)
);
