-- https://github.com/ClickHouse/ClickHouse/issues/91119
--
-- A correlated subquery references an outer column that becomes `Nullable` after
-- the outer `GROUP BY` with `WITH ROLLUP`/`CUBE`/`GROUPING SETS` and `group_by_use_nulls = 1`.
-- The analyzer used to leave the correlated column reference at its non-Nullable type,
-- so the planner built CAST wrappers and aggregate functions for the original signature.
-- At header inference time the actual input column was already `Nullable`, which produced
-- "Bad cast from type ColumnNullable to ColumnVector<...>" exceptions.
--
-- The fix updates the analyzer so a correlated column reference picks up the Nullable
-- type from the outer scope's `nullable_group_by_keys` set before the planner runs.
--
-- ClickHouse currently rejects correlated subqueries in ORDER BY directly, so each query
-- below either does not need ordering or sorts by a GROUP BY key that is now Nullable.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET group_by_use_nulls = 1;

SELECT '-- ROLLUP ---';
SELECT (SELECT c0)
FROM (SELECT 1::Bool) t0(c0)
GROUP BY c0 WITH ROLLUP
ORDER BY c0 NULLS LAST;

SELECT '-- CUBE ---';
SELECT (SELECT c0)
FROM (SELECT 1::Bool) t0(c0)
GROUP BY c0 WITH CUBE
ORDER BY c0 NULLS LAST;

SELECT '-- GROUPING SETS ---';
SELECT (SELECT c0)
FROM (SELECT 1::Bool) t0(c0)
GROUP BY GROUPING SETS ((c0), ())
ORDER BY c0 NULLS LAST;

SELECT '-- numeric ---';
SELECT (SELECT c0)
FROM (SELECT 1) t0(c0)
GROUP BY c0 WITH ROLLUP
ORDER BY c0 NULLS LAST;

SELECT '-- complex expression ---';
SELECT (SELECT c0 + 1)
FROM (SELECT toInt32(1)) t0(c0)
GROUP BY c0 WITH ROLLUP
ORDER BY c0 NULLS LAST;

SELECT '-- modulo ---';
SELECT (SELECT c0 % 2)
FROM (SELECT toInt32(3)) t0(c0)
GROUP BY c0 WITH ROLLUP
ORDER BY c0 NULLS LAST;

SELECT '-- multiple GROUP BY keys ---';
SELECT number, (SELECT number % 2), sum(number)
FROM numbers(3)
GROUP BY number
WITH ROLLUP
ORDER BY number NULLS LAST;

SELECT '-- aggregate over correlated column ---';
SELECT number, (SELECT sum(number))
FROM numbers(3)
GROUP BY number
WITH ROLLUP
ORDER BY number NULLS LAST;

SELECT '-- HAVING with correlated subquery ---';
SELECT number, sum(number) AS s
FROM numbers(3)
GROUP BY number
WITH ROLLUP
HAVING s >= (SELECT number)
ORDER BY number NULLS LAST;

SELECT '-- WHERE with correlated subquery ---';
SELECT number, sum(number) AS s
FROM numbers(3)
WHERE number >= (SELECT number - 1)
GROUP BY number
WITH ROLLUP
ORDER BY number NULLS LAST;

SELECT '-- Nullable source ---';
SELECT (SELECT c0)
FROM (SELECT toNullable(1::Bool)) t0(c0)
GROUP BY c0 WITH ROLLUP
ORDER BY c0 NULLS LAST;

-- Aggregate over a correlated String key: exercises the ColumnNullable -> ColumnString wrapper
-- rather than the numeric ColumnVector path.
SELECT '-- min over correlated String ---';
SELECT (SELECT min(c0))
FROM (SELECT 'a'::String) t0(c0)
GROUP BY c0 WITH ROLLUP
ORDER BY c0 NULLS LAST;

SELECT '-- minOrDefault over correlated String with GROUPING SETS ---';
SELECT (SELECT minOrDefault(c0))
FROM (SELECT 'a'::String) t0(c0)
GROUP BY GROUPING SETS ((c0), ())
ORDER BY c0 NULLS LAST;

-- Sanity check: a non-correlated subquery in the same context must NOT have its inner
-- column wrapped as Nullable just because the outer query has `group_by_use_nulls + ROLLUP`.
-- The two `c0` columns happen to share a name; the inner one resolves to its own table
-- expression and stays a plain `String`.
SELECT '-- non-correlated subquery ---';
SELECT (SELECT c0 FROM (SELECT 'inner'::String) t1(c0))
FROM (SELECT 1::Bool) t0(c0)
GROUP BY c0 WITH ROLLUP
ORDER BY c0 NULLS LAST;
