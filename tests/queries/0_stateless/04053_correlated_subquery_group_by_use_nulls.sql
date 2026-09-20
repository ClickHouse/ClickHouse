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

-- Nested correlated subquery where the outermost query has `group_by_use_nulls`
-- + ROLLUP/CUBE on a column with the SAME NAME as a column source in the middle
-- query. `nullable_group_by_keys` is keyed by query-tree structure (column name,
-- ignoring source and types), so without the source-scope stop condition the
-- inner expression could pick up the outer query's nullable-key entry and
-- wrongly convert the middle table's column to `Nullable`.
SELECT a.number,
       (SELECT (SELECT b.number) FROM numbers(2) AS b LIMIT 1) AS val
FROM numbers(3) AS a
GROUP BY a.number
WITH ROLLUP
ORDER BY a.number ASC NULLS FIRST;

-- The SAME correlated outer column referenced multiple times inside one inner
-- expression. The correlated column is converted to `Nullable` in place during
-- resolution, so de-duplication in `QueryNode::addCorrelatedColumn` must stay
-- stable across that rewrite; otherwise the column would be appended twice and
-- produce duplicate correlated keys in the decorrelated `AggregatingStep`.
SELECT number, (SELECT number + number + 1) AS val
FROM numbers(4)
GROUP BY number
WITH ROLLUP
ORDER BY number ASC NULLS FIRST;

SELECT number, (SELECT number % 2 + number * 3) AS val
FROM numbers(4)
GROUP BY number
WITH CUBE
ORDER BY number ASC NULLS FIRST;

-- Repeated correlated column inside an aggregate function in the inner subquery.
SELECT number, (SELECT sum(number + number) AS val)
FROM numbers(3)
GROUP BY number
WITH CUBE
ORDER BY number ASC NULLS LAST;

-- Repeated correlated column inside the inner subquery's own `WHERE`, exercising
-- the join-predicate / key-extension path of decorrelation.
SELECT a.number, (SELECT sum(b.number) FROM numbers(5) AS b WHERE b.number < a.number + a.number) AS val
FROM numbers(4) AS a
GROUP BY a.number
WITH ROLLUP
ORDER BY a.number ASC NULLS FIRST;

-- A correlated column feeding a type-sensitive parent function (`if`/`multiIf`/`tuple`)
-- while a sibling occurrence sits inside a comparison: the direct occurrence becomes
-- `Nullable` while the comparison stays non-`Nullable`, so the parent function's
-- return type must reflect the `Nullable` branch. These used to throw
-- "Unexpected return type from if. Expected UInt64. Got Nullable(UInt64)".
SELECT number, (SELECT if(number > 2, 5, number)), sum(number) AS val
FROM numbers(6)
GROUP BY number
WITH ROLLUP
ORDER BY number ASC NULLS FIRST;

SELECT number, (SELECT if(number > 2, number, 0))
FROM numbers(4)
GROUP BY number
WITH CUBE
ORDER BY number ASC NULLS FIRST;

SELECT number, (SELECT multiIf(number > 2, number, number < 1, 100, 0))
FROM numbers(4)
GROUP BY number
WITH ROLLUP
ORDER BY number ASC NULLS FIRST;

SELECT number, (SELECT (number, number > 2))
FROM numbers(4)
GROUP BY number
WITH ROLLUP
ORDER BY number ASC NULLS FIRST;
