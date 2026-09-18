-- https://github.com/ClickHouse/ClickHouse/issues/110915
--
-- An aggregate reads a derived table that uses GROUP BY ALL WITH ROLLUP under
-- group_by_use_nulls = 1. GROUP BY ALL is expanded into the key list only at the end of query
-- resolution, so its keys were never registered as nullable_group_by_keys and the inner
-- projection kept its non-Nullable type while WITH ROLLUP execution produced Nullable. The
-- aggregate was then built for the non-Nullable argument, without the Null combinator adapter,
-- and raised "Bad cast from type ColumnNullable to ColumnVector<...>" at run time. Spelling the
-- key explicitly (GROUP BY val) already worked; this test covers GROUP BY ALL.

SET enable_analyzer = 1;
SET group_by_use_nulls = 1;

-- The inner projection must be Nullable, exactly as with an explicit GROUP BY key.
SELECT '-- inner projection type --';
SELECT DISTINCT toTypeName(val) FROM (SELECT materialize(1::UInt64) AS val FROM numbers(3) GROUP BY ALL WITH ROLLUP);

-- The reported minimizer and its sibling aggregates/combinators (was: Bad cast exception).
SELECT '-- scalar subquery aggregates --';
SELECT (SELECT avgOrDefault(val) FROM (SELECT materialize(2::UInt64) AS val FROM numbers(3) GROUP BY ALL WITH ROLLUP));
SELECT (SELECT sum(val)          FROM (SELECT materialize(2::UInt64) AS val FROM numbers(3) GROUP BY ALL WITH ROLLUP));
SELECT (SELECT minOrDefault(val) FROM (SELECT materialize(2::UInt64) AS val FROM numbers(3) GROUP BY ALL WITH ROLLUP));
SELECT (SELECT maxOrNull(val)    FROM (SELECT materialize(2::UInt64) AS val FROM numbers(3) GROUP BY ALL WITH ROLLUP));

-- Deterministic-key variant that used to fail during analysis with NOT_AN_AGGREGATE.
SELECT '-- deterministic key --';
SELECT (SELECT avgOrDefault(val) FROM (SELECT sipHash64(number) AS val FROM numbers(10) GROUP BY ALL WITH ROLLUP)) FORMAT Null;

-- The aggregate does not have to be in a scalar subquery, and the argument type is not limited
-- to integers. https://github.com/ClickHouse/ClickHouse/issues/113078
SELECT '-- top-level aggregate over the derived table --';
SELECT max(x) FROM (SELECT materialize(1.5) AS x GROUP BY ALL WITH ROLLUP);
SELECT sum(x) FROM (SELECT materialize(7::UInt64) AS x GROUP BY ALL WITH CUBE);

-- isNull is folded against the declared type, so without the promotion the super-aggregate row
-- is reported as not NULL and the query returns a wrong result instead of raising anything.
SELECT '-- isNull on the super-aggregate row --';
SELECT x, isNull(x) FROM (SELECT materialize(1.5) AS x GROUP BY ALL WITH ROLLUP) ORDER BY x NULLS LAST;

-- GROUP BY ALL WITH ROLLUP must match explicit GROUP BY, including the Nullable super-aggregate row.
SELECT '-- group by all matches explicit key --';
SELECT k, sum(v) FROM (SELECT number % 3 AS k, number AS v FROM numbers(9)) GROUP BY ALL WITH ROLLUP ORDER BY k NULLS LAST, 2;
SELECT '----';
SELECT k, sum(v) FROM (SELECT number % 3 AS k, number AS v FROM numbers(9)) GROUP BY k   WITH ROLLUP ORDER BY k NULLS LAST, 2;

-- A correlated subquery reading the key sees it as Nullable as well, so the return type of a
-- function applied to it stays consistent with the decorrelated expression. The explicit-GROUP-BY
-- spelling is covered by 04516_correlated_subquery_return_type_group_by_use_nulls.
SET allow_experimental_correlated_subqueries = 1;
SELECT '-- correlated subquery over the key --';
SELECT number, (SELECT toString(number)) FROM numbers(3) GROUP BY ALL WITH ROLLUP ORDER BY number ASC NULLS LAST;
SELECT '----';
SELECT number, (SELECT toString(number)) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY number ASC NULLS LAST;
