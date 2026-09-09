-- Regression: AFTER/UNTIL boundary expressions may reference columns not in the SELECT list.
-- The analyzer-side preservation step must keep those columns alive through LimitRangeStep
-- without dropping the selected output columns.

-- { echo }

-- AFTER references non-selected column y (analyzer).
SELECT x FROM (SELECT number AS x, number * 2 AS y FROM numbers(10)) ORDER BY x LIMIT 3 AFTER y >= 10;

-- AFTER + UNTIL both reference non-selected column y (analyzer).
SELECT x FROM (SELECT number AS x, number * 2 AS y FROM numbers(10)) ORDER BY x LIMIT AFTER y >= 10 UNTIL y >= 14;

-- AFTER references non-selected column y, no count (analyzer).
SELECT x FROM (SELECT number AS x, number * 2 AS y FROM numbers(10)) ORDER BY x LIMIT AFTER y >= 16;

-- AFTER ALL references non-selected column y (analyzer).
SELECT x FROM (SELECT number AS x, number * 2 AS y FROM numbers(10)) ORDER BY x LIMIT 2 AFTER y IN (6, 14) ALL;

-- Multiple selected columns, boundary on a non-selected one (analyzer).
SELECT x, x + 1 AS z FROM (SELECT number AS x, number * 2 AS y FROM numbers(10)) ORDER BY x LIMIT 3 AFTER y >= 10;

-- Same queries with legacy interpreter.
SET enable_analyzer = 0;

SELECT x FROM (SELECT number AS x, number * 2 AS y FROM numbers(10)) ORDER BY x LIMIT 3 AFTER y >= 10;
SELECT x FROM (SELECT number AS x, number * 2 AS y FROM numbers(10)) ORDER BY x LIMIT AFTER y >= 10 UNTIL y >= 14;
SELECT x FROM (SELECT number AS x, number * 2 AS y FROM numbers(10)) ORDER BY x LIMIT AFTER y >= 16;
SELECT x FROM (SELECT number AS x, number * 2 AS y FROM numbers(10)) ORDER BY x LIMIT 2 AFTER y IN (6, 14) ALL;
SELECT x, x + 1 AS z FROM (SELECT number AS x, number * 2 AS y FROM numbers(10)) ORDER BY x LIMIT 3 AFTER y >= 10;
