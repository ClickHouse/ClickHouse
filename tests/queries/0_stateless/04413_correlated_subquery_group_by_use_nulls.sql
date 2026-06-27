-- Regression test: a correlated scalar subquery that references an outer GROUP BY key
-- used to abort with a logical error
--   "Unexpected return type from toString. Expected String. Got Nullable(String)"
-- when `group_by_use_nulls = 1` made the grouping key Nullable (GROUPING SETS / ROLLUP /
-- CUBE / WITH TOTALS). The correlated column stayed non-Nullable while the decorrelated
-- plan fed in the real Nullable column, so the baked result type no longer matched.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET group_by_use_nulls = 1;

-- The correlated column must be Nullable, hence toString over it must be Nullable(String).
SELECT DISTINCT toTypeName((SELECT toString(number))) AS t
FROM numbers(3)
GROUP BY GROUPING SETS ((number));

-- GROUPING SETS
SELECT number, concat('s', (SELECT toString(number))) AS c
FROM numbers(3)
GROUP BY GROUPING SETS ((number))
ORDER BY number;

-- GROUPING SETS WITH TOTALS
SELECT number, concat('s', (SELECT toString(number))) AS c
FROM numbers(3)
GROUP BY GROUPING SETS ((number))
WITH TOTALS
ORDER BY number;

-- ROLLUP: the super-aggregate row has number = NULL, so the correlated value is NULL.
SELECT number, (SELECT toString(number)) AS c
FROM numbers(3)
GROUP BY number WITH ROLLUP
ORDER BY number NULLS LAST;

-- CUBE
SELECT number, (SELECT toString(number)) AS c
FROM numbers(2)
GROUP BY number WITH CUBE
ORDER BY number NULLS LAST;

-- A non-correlated subquery in the same shape must be unaffected by group_by_use_nulls.
SELECT number, (SELECT 42) AS c
FROM numbers(3)
GROUP BY number WITH ROLLUP
ORDER BY number NULLS LAST;

-- The exact query shape the CI AST fuzzer aborted on (https://github.com/ClickHouse/ClickHouse/pull/108685),
-- scaled down and without FORMAT CSV: a multi-column projection with a correlated scalar subquery over a
-- GROUPING SETS key under WITH TOTALS.
SELECT number, number + 1, concat('string: ', (SELECT toString(number))) AS c
FROM numbers(3)
GROUP BY GROUPING SETS ((number))
WITH TOTALS
ORDER BY number;
