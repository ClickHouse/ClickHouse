-- Correlated scalar subquery applying a return-type-sensitive function to the
-- outer key, combined with WITH ROLLUP + group_by_use_nulls, used to fail with
-- "Unexpected return type from <fn>. Expected <T>. Got Nullable(<T>)" because
-- the decorrelated expression DAG kept the pre-Nullable result type for the
-- function while the correlated key became Nullable. See issue #107445.

SET enable_analyzer = 1;
SET group_by_use_nulls = 1;
SET allow_experimental_correlated_subqueries = 1;

SELECT number, (SELECT toString(number))
FROM numbers(10)
GROUP BY number WITH ROLLUP
ORDER BY number ASC NULLS LAST;

SELECT number, (SELECT tuple(number))
FROM numbers(10)
GROUP BY number WITH ROLLUP
ORDER BY number ASC NULLS LAST;

SELECT number, (SELECT toInt64(number))
FROM numbers(10)
GROUP BY number WITH ROLLUP
ORDER BY number ASC NULLS LAST;

SELECT number, (SELECT toString(number))
FROM numbers(5)
GROUP BY number WITH CUBE
ORDER BY number ASC NULLS LAST;

-- Original AST-fuzzer query (return-type-sensitive function nested in concat,
-- WITH ROLLUP WITH TOTALS).
SELECT number, number + 1, concat('192.168.0.1', (SELECT toString(number)))
FROM numbers(5)
GROUP BY number WITH ROLLUP WITH TOTALS
ORDER BY number ASC NULLS LAST;
