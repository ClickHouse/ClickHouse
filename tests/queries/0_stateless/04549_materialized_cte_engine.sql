-- Custom table engines (Memory, Join, Set) for materialized CTEs:
--   WITH t AS MATERIALIZED ENGINE = <Engine>[(args)] (subquery)

SET enable_materialized_cte = 1;

-- Explicit ENGINE = Memory behaves as the default; referenced twice so it materializes.
WITH t AS MATERIALIZED ENGINE = Memory (SELECT number AS n FROM numbers(5))
SELECT (SELECT sum(n) FROM t) AS a, (SELECT count() FROM t) AS b;

-- Set engine consumed in IN, referenced twice (materialized, not inlined).
WITH s AS MATERIALIZED ENGINE = Set (SELECT number FROM numbers(10))
SELECT
    (SELECT count() FROM numbers(100) WHERE number IN s) AS c1,
    (SELECT count() FROM numbers(5) WHERE number IN s) AS c2;

-- Join engine consumed in a JOIN, referenced twice.
WITH j AS MATERIALIZED ENGINE = Join(ANY, LEFT, k) (SELECT number AS k, number * 2 AS v FROM numbers(5))
SELECT
    (SELECT sum(v) FROM (SELECT number AS k FROM numbers(3)) AS l ANY LEFT JOIN j USING (k)) AS s1,
    (SELECT count() FROM (SELECT number AS k FROM numbers(3)) AS l ANY LEFT JOIN j USING (k)) AS s2;

-- Single-use CTEs are inlined regardless of the specified engine; results match the plain form.
WITH s AS MATERIALIZED ENGINE = Set (SELECT number FROM numbers(10))
SELECT count() FROM numbers(100) WHERE number IN s;

WITH j AS MATERIALIZED ENGINE = Join(ANY, LEFT, k) (SELECT number AS k, number * 2 AS v FROM numbers(5))
SELECT sum(v) FROM (SELECT number AS k FROM numbers(3)) AS l ANY LEFT JOIN j USING (k);

-- The ENGINE clause parses and round-trips (in particular ENGINE = Join(args) is not mis-parsed
-- as a parametric function that swallows the CTE subquery).
SELECT formatQuery('WITH t AS MATERIALIZED ENGINE = Memory (SELECT 1) SELECT * FROM t');
SELECT formatQuery('WITH s AS MATERIALIZED ENGINE = Set (SELECT 1 AS x) SELECT 1 IN s');
SELECT formatQuery('WITH j AS MATERIALIZED ENGINE = Join(ANY, LEFT, k) (SELECT 1 AS k) SELECT * FROM j');

-- With the feature disabled the ENGINE clause still parses; the CTE is treated as a normal one.
WITH s AS MATERIALIZED ENGINE = Set (SELECT number FROM numbers(10))
SELECT count() FROM numbers(100) WHERE number IN s
SETTINGS enable_materialized_cte = 0;

-- Only Memory, Join and Set are allowed.
WITH t AS MATERIALIZED ENGINE = Log (SELECT number FROM numbers(5))
SELECT count() FROM t; -- { serverError BAD_ARGUMENTS }

-- A Set-engine CTE cannot be read as a table (two references keep it materialized as a Set).
WITH s AS MATERIALIZED ENGINE = Set (SELECT number FROM numbers(10))
SELECT count() FROM s AS a, s AS b; -- { serverError NOT_IMPLEMENTED }
