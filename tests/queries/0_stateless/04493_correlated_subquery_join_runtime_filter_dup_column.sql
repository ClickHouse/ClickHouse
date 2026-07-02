-- A correlated subquery over a relation whose header carries a column name twice
-- (`SELECT number, *` yields two columns named `number`) is decorrelated into an
-- ANY RIGHT JOIN. With join runtime filters enabled the filter is built on that
-- duplicated key column, changing a downstream join input's column multiplicity and
-- aborting with `Block structure mismatch in JoinStep` in debug/sanitizer builds.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET enable_join_runtime_filters = 1;

-- { echoOn }
WITH t AS (SELECT number, * FROM numbers(3))
SELECT *, (SELECT t.number WHERE t.number >= 0) AS r FROM t
ORDER BY 1
SETTINGS correlated_subqueries_default_join_kind = 'right', correlated_subqueries_use_in_memory_buffer = 0;

WITH t AS (SELECT number, * FROM numbers(3))
SELECT *, (SELECT t.number WHERE t.number >= 0) AS r FROM t
ORDER BY 1
SETTINGS correlated_subqueries_default_join_kind = 'left', correlated_subqueries_use_in_memory_buffer = 0;

WITH t AS (SELECT number, *, * FROM numbers(3))
SELECT *, (SELECT t.number WHERE t.number >= 0) AS r FROM t
ORDER BY 1
SETTINGS correlated_subqueries_default_join_kind = 'right', correlated_subqueries_use_in_memory_buffer = 0;
-- { echoOff }

-- A runtime filter is still built for a normal join without duplicated column names.
SELECT countIf(explain LIKE '%BuildRuntimeFilter%') > 0
FROM (
    EXPLAIN PLAN
    SELECT * FROM (SELECT number AS a FROM numbers(100)) AS l
    ANY RIGHT JOIN (SELECT number AS b FROM numbers(3)) AS r ON l.a = r.b
    SETTINGS enable_join_runtime_filters = 1
);

-- The filter must still be built when the build side has a duplicated NON-key column
-- (here the key `b` is unique and `c` is duplicated): only a duplicated KEY column breaks the
-- name-keyed filter machinery, so the guard is scoped to the join keys and must not disable the
-- filter for an unrelated duplicated column.
SELECT countIf(explain LIKE '%BuildRuntimeFilter%') > 0
FROM (
    EXPLAIN PLAN
    SELECT * FROM (SELECT number AS a FROM numbers(100)) AS l
    ANY RIGHT JOIN (SELECT number AS b, number + 1 AS c, c FROM numbers(3)) AS r ON l.a = r.b
    SETTINGS enable_join_runtime_filters = 1
);
