-- Aggregate combinators (e.g. -Distinct) inside a correlated scalar subquery over a
-- GROUP BY key that becomes Nullable via group_by_use_nulls + ROLLUP/CUBE used to abort with
-- 'Bad cast from type DB::ColumnNullable to DB::ColumnVector<...>' (debug), or silently
-- reinterpret the column (release). Fixed by #100365 at the analyzer level; this asserts
-- correct results across a few combinators, which also guards the release silent-UB path.

-- Correlated subqueries are a new-analyzer feature; force it so the old-analyzer CI job
-- does not fail with UNKNOWN_IDENTIFIER instead of exercising the fix.
SET enable_analyzer = 1;

SELECT number, (SELECT sumDistinct(number)) AS s FROM numbers(1) GROUP BY number WITH ROLLUP ORDER BY number NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT number, (SELECT sumDistinct(number)) AS s
FROM numbers(3) GROUP BY number WITH ROLLUP
ORDER BY number NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT number, (SELECT countDistinct(number)) AS c
FROM numbers(3) GROUP BY number WITH CUBE
ORDER BY number NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT number, (SELECT maxDistinct(number)) AS m
FROM numbers(3) GROUP BY number WITH ROLLUP
ORDER BY number NULLS LAST SETTINGS group_by_use_nulls = 1;
