-- Custom table engines (Memory, Set) for materialized CTEs:
--   WITH t AS MATERIALIZED ENGINE = <Engine> (subquery)

SET enable_materialized_cte = 1;

-- Explicit ENGINE = Memory behaves as the default; referenced twice so it materializes.
WITH t AS MATERIALIZED ENGINE = Memory (SELECT number AS n FROM numbers(5))
SELECT (SELECT sum(n) FROM t) AS a, (SELECT count() FROM t) AS b;

-- Set engine consumed in IN, referenced twice (materialized, not inlined).
WITH s AS MATERIALIZED ENGINE = Set (SELECT number FROM numbers(10))
SELECT
    (SELECT count() FROM numbers(100) WHERE number IN s) AS c1,
    (SELECT count() FROM numbers(5) WHERE number IN s) AS c2;

-- Set engine referenced twice directly in the main query (not in subqueries).
WITH s AS MATERIALIZED ENGINE = Set (SELECT number FROM numbers(10))
SELECT countIf(number IN s) AS in_s, countIf(number NOT IN s) AS not_in_s FROM numbers(20);

-- Single-use CTE is inlined regardless of the specified engine; result matches the plain form.
WITH s AS MATERIALIZED ENGINE = Set (SELECT number FROM numbers(10))
SELECT count() FROM numbers(100) WHERE number IN s;

-- The ENGINE clause parses and round-trips.
SELECT formatQuery('WITH t AS MATERIALIZED ENGINE = Memory (SELECT 1) SELECT * FROM t');
SELECT formatQuery('WITH s AS MATERIALIZED ENGINE = Set (SELECT 1 AS x) SELECT 1 IN s');

-- With the feature disabled the ENGINE clause still parses; the CTE is treated as a normal one.
WITH s AS MATERIALIZED ENGINE = Set (SELECT number FROM numbers(10))
SELECT count() FROM numbers(100) WHERE number IN s
SETTINGS enable_materialized_cte = 0;

-- Only Memory and Set are allowed (Join is not supported yet).
WITH t AS MATERIALIZED ENGINE = Log (SELECT number FROM numbers(5))
SELECT count() FROM t; -- { serverError BAD_ARGUMENTS }

WITH j AS MATERIALIZED ENGINE = Join(ANY, LEFT, k) (SELECT number AS k FROM numbers(5))
SELECT count() FROM (SELECT 1 AS k) AS l ANY LEFT JOIN j USING (k); -- { serverError BAD_ARGUMENTS }

-- A SETTINGS clause on the engine is not parsed, so it is a syntax error.
WITH s AS MATERIALIZED ENGINE = Set SETTINGS x = 1 (SELECT number FROM numbers(10)) SELECT count() FROM numbers(10) WHERE number IN s; -- { clientError SYNTAX_ERROR }

-- A Set-engine CTE cannot be read as a table (two references keep it materialized as a Set).
WITH s AS MATERIALIZED ENGINE = Set (SELECT number FROM numbers(10))
SELECT count() FROM s AS a, s AS b; -- { serverError NOT_IMPLEMENTED }
