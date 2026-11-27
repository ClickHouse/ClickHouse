-- Random settings limits: optimize_rewrite_sum_if_to_count_if=(1, 1)
-- An alias of sumIf(k, cond) or sum(if(cond, k, 0)) referenced more than once must return the same value at every reference.

SET enable_identifier_resolve_cache = 1;

SELECT '-- HAVING';
SELECT sumIf(2, number > 1) AS r FROM numbers(12) HAVING r > 3 AND r < 15;
SELECT number % 3 AS g, sumIf(2, number > 1) AS r FROM numbers(12) GROUP BY g HAVING r > 3 AND r < 7 ORDER BY g;

SELECT '-- projection';
SELECT sumIf(2, number > 1) AS r, r + 0, r + 1 FROM numbers(12);
SELECT sum(if(number > 1, 0, 2)) AS r, r + 0, r + 1 FROM numbers(12);
SELECT sum(if(number > 1, 2, 0)) AS r, r + 0, r + 1 FROM numbers(12) SETTINGS optimize_rewrite_aggregate_function_with_if = 0;

SELECT '-- every reference is rewritten';
SELECT countIf(explain LIKE '%function_name: multiply%'), countIf(explain LIKE '%function_name: countIf%'), countIf(explain LIKE '%function_name: sumIf%')
FROM (EXPLAIN QUERY TREE SELECT sumIf(2, number > 1) AS r FROM numbers(12) HAVING r > 3 AND r < 15);
