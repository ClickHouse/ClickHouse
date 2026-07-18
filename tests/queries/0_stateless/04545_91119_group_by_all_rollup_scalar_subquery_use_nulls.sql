-- https://github.com/ClickHouse/ClickHouse/issues/110915
--
-- A scalar subquery aggregates over a derived table that uses GROUP BY ALL WITH ROLLUP
-- under group_by_use_nulls = 1. GROUP BY ALL is expanded only at the end of query
-- resolution, so its keys were never registered as nullable_group_by_keys and the inner
-- projection kept its non-Nullable type while WITH ROLLUP execution produced Nullable.
-- The outer aggregate was then built for the non-Nullable argument (no Null adapter) and
-- raised a "Bad cast from type ColumnNullable to ColumnVector<...>" exception at run time.
-- Spelling the key explicitly (GROUP BY val) already worked; this test covers GROUP BY ALL.

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

-- GROUP BY ALL WITH ROLLUP must match explicit GROUP BY, including the Nullable super-aggregate row.
SELECT '-- group by all matches explicit key --';
SELECT k, sum(v) FROM (SELECT number % 3 AS k, number AS v FROM numbers(9)) GROUP BY ALL WITH ROLLUP ORDER BY k NULLS LAST, 2;
SELECT '----';
SELECT k, sum(v) FROM (SELECT number % 3 AS k, number AS v FROM numbers(9)) GROUP BY k   WITH ROLLUP ORDER BY k NULLS LAST, 2;
