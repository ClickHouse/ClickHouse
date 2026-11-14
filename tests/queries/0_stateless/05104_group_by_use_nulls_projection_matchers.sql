-- With `group_by_use_nulls`, the projection is resolved after the other clauses of the query, so that
-- GROUP BY keys can be wrapped into Nullable. Matchers still have to be expanded first, exactly as
-- without the setting: `SELECT * REPLACE (...)` rewrites the replaced names in the other clauses while
-- they are still unresolved, and positional arguments count the expanded columns.

SET enable_analyzer = 1;
SET group_by_use_nulls = 1;

SELECT '-- REPLACE is visible in ORDER BY, HAVING, WHERE, GROUP BY and a named WINDOW';
SELECT * REPLACE (-c AS c) FROM (SELECT number AS c FROM numbers(4)) GROUP BY GROUPING SETS ((), (c)) ORDER BY c;
SELECT * REPLACE (-c AS c) FROM (SELECT number AS c FROM numbers(4)) GROUP BY c WITH CUBE HAVING c < -1 ORDER BY c;
SELECT * REPLACE (-c AS c) FROM (SELECT number AS c FROM numbers(4)) WHERE c > -2 GROUP BY c WITH ROLLUP ORDER BY c;
SELECT * REPLACE (intDiv(c, 2) AS c) FROM (SELECT number AS c FROM numbers(4)) GROUP BY GROUPING SETS ((), (c)) ORDER BY c;
SELECT * REPLACE (-c AS c), row_number() OVER w FROM (SELECT number AS c FROM numbers(4)) GROUP BY GROUPING SETS ((), (c)) WINDOW w AS (ORDER BY c) ORDER BY c;

SELECT '-- Later projection items and later matchers see the earlier REPLACE';
SELECT * REPLACE (10 AS c), c + 1 FROM (SELECT 1 AS c) GROUP BY c WITH ROLLUP SETTINGS enable_positional_arguments = 0;
SELECT k, COLUMNS('^b$') REPLACE (max(a) + 1 AS b), COLUMNS('^a$') REPLACE (0 AS a)
FROM (SELECT 1 AS k, -5 AS a, 7 AS b) GROUP BY k WITH ROLLUP HAVING b > 0 ORDER BY k SETTINGS enable_positional_arguments = 0;

SELECT '-- Positional arguments refer to the expanded columns';
SELECT number, * FROM numbers(2) GROUP BY 2 WITH ROLLUP ORDER BY 1;
SELECT * APPLY toString FROM (SELECT 1 AS c) GROUP BY GROUPING SETS ((1), ()) ORDER BY 1;

SELECT '-- The result of APPLY may be a GROUP BY key';
SELECT toTypeName(*) FROM (SELECT * APPLY isNull FROM (SELECT 1::UInt8 AS c) GROUP BY GROUPING SETS ((), (1)));
SELECT min(*) FROM (SELECT * APPLY toTypeName FROM (SELECT 1 AS c) GROUP BY GROUPING SETS ((), (1)));
SELECT minOrNull(*) FROM (SELECT * APPLY toTypeName FROM (SELECT 1 AS c) GROUP BY GROUPING SETS ((), (1)));

SELECT '-- The argument of grouping stays a GROUP BY key';
SELECT * APPLY x -> grouping(x) FROM numbers(1) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT * APPLY grouping FROM numbers(1) GROUP BY number WITH CUBE ORDER BY 1;

SELECT '-- Aggregates and plain functions in APPLY';
SELECT * APPLY x -> argMax(x, number) FROM numbers(1) GROUP BY GROUPING SETS ((materialize(65537)), (*));
SELECT * APPLY x -> sum(x), * APPLY toString FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY number;

SELECT '-- The same column node inside and outside of an aggregate function';
SELECT * APPLY x -> tuple(sum(x), x) FROM numbers(2) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT * APPLY sum, * FROM numbers(2) GROUP BY number WITH ROLLUP ORDER BY 1;
