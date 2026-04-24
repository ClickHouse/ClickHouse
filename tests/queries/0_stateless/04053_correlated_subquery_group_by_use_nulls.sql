-- Correlated subquery with group_by_use_nulls + WITH ROLLUP used to fail with:
-- "Unexpected return type from modulo. Expected UInt8. Got Nullable(UInt8)"
-- because the decorrelated expression DAG kept the pre-Nullable types for
-- correlated columns that became Nullable due to ROLLUP + group_by_use_nulls.

SET enable_analyzer = 1;
SET group_by_use_nulls = 1;

SELECT number, (SELECT number % 2), sum(number) AS val
FROM numbers(10)
GROUP BY number, number % 2
WITH ROLLUP WITH TOTALS
ORDER BY (number, number % 2, val) ASC NULLS FIRST;

SELECT number, (SELECT number + 1), sum(number) AS val
FROM numbers(5)
GROUP BY number
WITH CUBE
ORDER BY number ASC NULLS FIRST;

-- Correlated subquery in HAVING exercises the `FilterStep` reconciliation path.
SELECT number, sum(number) AS val
FROM numbers(10)
GROUP BY number
WITH ROLLUP
HAVING val > (SELECT number - 1)
ORDER BY number ASC NULLS FIRST;

-- Correlated subquery in WHERE (pre-aggregation filter) under `group_by_use_nulls` + ROLLUP.
SELECT number, sum(number) AS val
FROM numbers(10)
WHERE number >= (SELECT number - 1)
GROUP BY number
WITH ROLLUP
ORDER BY number ASC NULLS FIRST;
