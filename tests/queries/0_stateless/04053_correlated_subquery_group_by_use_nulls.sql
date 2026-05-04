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

-- Correlated subquery with an aggregate function over the outer column under CUBE used to fail
-- with "Bad cast from type ColumnNullable to ColumnVector<UInt64>" because the inner aggregate
-- function `anyLastOrDefault` was bound at analysis time to the original non-Nullable type.
-- See https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=100365&sha=5ac64a85eca68d3b5f67201f4c9fcc381dc54007&name_0=PR&name_1=AST%20fuzzer%20%28amd_debug%2C%20targeted%29
SELECT number, (SELECT anyLastOrDefault(number) AS val)
FROM numbers(3)
GROUP BY number
WITH CUBE
ORDER BY number ASC NULLS LAST;

SELECT number, (SELECT sum(number) AS val)
FROM numbers(3)
GROUP BY number
WITH ROLLUP
ORDER BY number ASC NULLS LAST;
