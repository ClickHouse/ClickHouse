-- Regression test for issue #50998: trailing comma in SELECT fails when last column has an alias

-- These were broken before the fix (aliased column + trailing comma + table function / qualified name)
SELECT sum(x) AS s, FROM (SELECT number AS x FROM numbers(3)) GROUP BY x ORDER BY x;
SELECT n AS m, FROM (SELECT 1 AS n);
SELECT 1 AS a, FROM system.one;

-- Multiple aliased columns with trailing comma
SELECT 1 AS a, 2 AS b, FROM system.one;

-- These should continue to work (FROM as a column name with trailing comma)
WITH 1 AS from SELECT from, FROM numbers(1);
WITH 1 AS from SELECT from, from + from, FROM numbers(1);

-- `from` as the very first (and possibly only) element of the list, with no
-- preceding comma at all, is just an ordinary identifier, not a trailing-comma
-- candidate.
WITH 1 AS from SELECT from;
WITH tuple(1) AS from SELECT from.1;
WITH [1] AS from SELECT 0, from[1];

-- FROM as a column, followed by trailing comma, table function
WITH 1 AS from SELECT from, from + from, from IN [0], FROM numbers(1);

-- Double trailing comma should fail
SELECT 1 AS a,, FROM system.one; -- { clientError SYNTAX_ERROR }

-- Non-FROM clause boundaries after a trailing comma: `WHERE`, `GROUP BY`, `ORDER BY` and
-- `SETTINGS` are just as ambiguous with a column name as `FROM` is, since none of them are
-- reserved words at the lexer level (aliased last column).
SELECT 1 AS a, WHERE 1;
SELECT 1 AS a, GROUP BY 1;
SELECT 1 AS a, ORDER BY 1;
SELECT 1 AS a, SETTINGS max_threads = 1;

-- Same non-FROM boundaries, but with a non-aliased last column (the pre-existing path).
WITH 1 AS to SELECT to, WHERE 1;
WITH 1 AS to SELECT to, SETTINGS max_threads = 1;

-- Implicit-SELECT queries (no `SELECT` keyword at all): the very first list
-- element is parsed starting at the first token of the whole query, i.e.
-- there is no previous token at all. This must not crash/hang.
SET implicit_select = 1;
1 AS from;
1 AS from, FROM numbers(1);
