-- Verify that aliased tuple expressions inside parentheses format and
-- re-parse correctly (AST formatting roundtrip).
-- The bug: (('a', 'b') AS x) was incorrectly treated as tuple(('a', 'b') AS x)
-- because the parser's special case for (tuple_literal) didn't account for aliases.
-- This is tested automatically in debug builds via the AST consistency check.

SELECT (('a', 'b') AS x);

-- The formatting roundtrip check in debug builds validates this during parsing,
-- even though the query itself fails at execution.
SELECT 1 FROM system.one AS t1 INNER JOIN system.one AS t2 ON (('a', 'b') AS a6); -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- Also verify EXCEPT ALL / INTERSECT ALL with parentheses works correctly.
SELECT 1 EXCEPT ALL SELECT 2;
(SELECT 1) EXCEPT ALL (SELECT 2);
(SELECT 1) EXCEPT DISTINCT (SELECT 2);
SELECT number FROM numbers(5) EXCEPT ALL SELECT number FROM numbers(3);
(SELECT number FROM numbers(5)) INTERSECT ALL (SELECT number FROM numbers(3, 5));
