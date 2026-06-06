-- Regression test for issue #50998: trailing comma in SELECT fails when last column has an alias

-- These were broken before the fix (aliased column + trailing comma + table function / qualified name)
SELECT sum(x) AS s, FROM (SELECT number AS x FROM numbers(3)) GROUP BY x ORDER BY x;
SELECT n AS m, FROM (SELECT 1 AS n);
SELECT 1 AS a, FROM system.one;

-- These should continue to work (FROM as a column name with trailing comma)
WITH 1 AS from SELECT from, FROM numbers(1);
WITH 1 AS from SELECT from, from + from, FROM numbers(1);

-- FROM as a column, followed by trailing comma, table function
WITH 1 AS from SELECT from, from + from, from IN [0], FROM numbers(1);
