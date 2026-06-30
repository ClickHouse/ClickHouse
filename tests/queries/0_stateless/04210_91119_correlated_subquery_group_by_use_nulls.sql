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

-- Multiple GROUP BY keys with CUBE and an aggregate over the correlated key (STID 2672-39ba).
-- CUBE makes both `number` and `number % 2` Nullable in the extra rows; the correlated
-- `(SELECT sum(number))` references the outer `number` key. `sum(number)` (a plain
-- aggregate, not the correlated subquery) is used in ORDER BY for a deterministic order.
SELECT '-- multi-key CUBE with aggregate over correlated column ---';
SELECT number, number % 2, (SELECT sum(number))
FROM numbers(10)
GROUP BY number % 2, number WITH CUBE
ORDER BY number, number % 2, sum(number) NULLS LAST;

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

-- A constant LowCardinality GROUP BY key (STID 3721-419e). ConstantNode::convertToNullable used
-- plain makeNullableSafe, which is a no-op on LowCardinality(T) (it cannot be inside Nullable), so
-- the key stayed non-Nullable while its rollup/grouping-sets rows were NULL at runtime, and an
-- aggregate reading the subquery's projected type bad-cast ColumnNullable to ColumnString.
SELECT '-- minSimpleState over LowCardinality constant key ---';
SELECT minSimpleState(*) FROM (SELECT DISTINCT toLowCardinality('1') GROUP BY GROUPING SETS ((1)));

SELECT '-- projected type is LowCardinality(Nullable) ---';
SELECT toTypeName(*) FROM (SELECT DISTINCT toLowCardinality('1') GROUP BY GROUPING SETS ((1)));

SELECT '-- min over LowCardinality constant key with ROLLUP ---';
SELECT min(*) FROM (SELECT DISTINCT toLowCardinality('1') GROUP BY ROLLUP(1));

SELECT '-- max over LowCardinality constant key with GROUPING SETS ---';
SELECT max(*) FROM (SELECT toLowCardinality('a') GROUP BY GROUPING SETS ((1)));

SELECT '-- original fuzzer query ---';
SELECT minDistinct(c0 >= (SELECT minSimpleState(*) FROM (SELECT DISTINCT toLowCardinality('1') GROUP BY GROUPING SETS ((1)))))
FROM (SELECT 1 AS c0 LIMIT 197) AS t0;

-- Contributed by @coderashed (the exact AST-fuzzer seed query from his duplicate PR #108714, which
-- he closed in favour of this one): a correlated scalar subquery `toString(number)` inside a String
-- function, over a `GROUPING SETS` key. Aborts on master with `Bad cast ColumnNullable to ColumnString`.
SELECT '-- correlated toString in concat over GROUPING SETS ---';
SELECT x FROM (
    SELECT concat('s', (SELECT toString(number))) AS x
    FROM numbers(5) GROUP BY GROUPING SETS ((number))
) ORDER BY x;

-- Contributed by @coderashed: a type-pinning regression check. The other cases above check query
-- results, which still pass if a future change silently stops wrapping the correlated key as
-- Nullable (the values are unchanged, only the declared type is wrong). These assert the analyzer
-- types directly via EXPLAIN QUERY TREE, so a regression that drops the Nullable wrapping fails with
-- a clean diff. `id`s are masked because they are not stable across runs.
SELECT '-- type pin: bare correlated column is Nullable, outer key is not ---';
SELECT replaceRegexpAll(trim(explain), 'id: [0-9]+', 'id: N') AS line
FROM (EXPLAIN QUERY TREE
    SELECT (SELECT c) FROM (SELECT toInt32(3) AS c) t GROUP BY c WITH ROLLUP)
WHERE explain ILIKE '%column_name: c,%result_type%';

SELECT '-- type pin: compound correlated key (modulo) is Nullable ---';
SELECT replaceRegexpAll(trim(explain), 'id: [0-9]+', 'id: N') AS line
FROM (EXPLAIN QUERY TREE
    SELECT (SELECT c % 2) FROM (SELECT toInt32(3) AS c) t GROUP BY c % 2 WITH ROLLUP)
WHERE explain ILIKE '%function_name: modulo%result_type%';

SELECT '-- type pin: aggregate argument stays non-Nullable ---';
SELECT replaceRegexpAll(trim(explain), 'id: [0-9]+', 'id: N') AS line
FROM (EXPLAIN QUERY TREE
    SELECT * APPLY x -> argMax(x, number) FROM numbers(1) GROUP BY number WITH ROLLUP)
WHERE explain ILIKE '%column_name: number,%result_type%';
