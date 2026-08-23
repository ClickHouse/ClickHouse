-- A correlated scalar subquery reading a `Bool` grouping key that `ROLLUP`, `CUBE` or `GROUPING SETS`
-- turn into `Nullable(Bool)` because of `group_by_use_nulls`.
-- It used to throw `Bad cast from type DB::ColumnNullable to DB::ColumnVector<char8_t>`.
-- https://github.com/ClickHouse/ClickHouse/issues/91119

SET group_by_use_nulls = 1;
SET enable_analyzer = 1;

SELECT (SELECT c0) FROM (SELECT 1::Bool) t0(c0) GROUP BY c0 WITH ROLLUP ORDER BY c0;
SELECT (SELECT c0) FROM (SELECT 1::Bool) t0(c0) GROUP BY c0 WITH CUBE ORDER BY c0;
SELECT (SELECT c0) FROM (SELECT 1::Bool) t0(c0) GROUP BY c0 WITH TOTALS ORDER BY c0;
SELECT (SELECT c0), count() FROM (SELECT 1::Bool) t0(c0) GROUP BY GROUPING SETS ((c0), ()) ORDER BY c0;

-- The same, but without rewriting the correlated subquery into a plain column reference,
-- so that the query goes through the decorrelation of the query plan.

SET correlated_subqueries_substitute_equivalent_expressions = 0;

SELECT (SELECT c0) FROM (SELECT 1::Bool) t0(c0) GROUP BY c0 WITH ROLLUP ORDER BY c0;
SELECT (SELECT c0) FROM (SELECT 1::Bool) t0(c0) GROUP BY c0 WITH CUBE ORDER BY c0;
SELECT (SELECT c0) FROM (SELECT 1::Bool) t0(c0) GROUP BY c0 WITH TOTALS ORDER BY c0;
SELECT (SELECT c0), count() FROM (SELECT 1::Bool) t0(c0) GROUP BY GROUPING SETS ((c0), ()) ORDER BY c0;

-- More than one grouping key, and a `Nullable(Bool)` source column.

SELECT c0, c1, (SELECT c0), (SELECT c1)
FROM (SELECT true AS c0, CAST(false, 'Nullable(Bool)') AS c1)
GROUP BY c0, c1 WITH ROLLUP ORDER BY c0, c1;

-- The result of the correlated subquery is `Nullable` as well.

SELECT DISTINCT toTypeName(x) FROM (SELECT (SELECT c0) AS x FROM (SELECT 1::Bool) t0(c0) GROUP BY c0 WITH ROLLUP);
