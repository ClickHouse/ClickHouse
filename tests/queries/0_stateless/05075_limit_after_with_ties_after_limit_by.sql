-- After `LIMIT BY`, the second `LIMIT` reads `WITH TIES` before a range, like the first `LIMIT` does, so
-- a query with both formats back in the order that reparses. `WITH TIES` is not supported together with
-- a range, which the planner reports.
SELECT formatQuery('SELECT number FROM numbers(5) ORDER BY number LIMIT 1 BY number LIMIT 2 WITH TIES AFTER number >= 1');
SELECT formatQuery(formatQuery('SELECT number FROM numbers(5) ORDER BY number LIMIT 1 BY number LIMIT 2 WITH TIES AFTER number >= 1'));
SELECT number FROM numbers(5) ORDER BY number LIMIT 1 BY number LIMIT 2 WITH TIES AFTER number >= 1; -- { serverError NOT_IMPLEMENTED }
SELECT number FROM numbers(5) ORDER BY number LIMIT 1 BY number LIMIT 2 AFTER number >= 1 WITH TIES; -- { clientError SYNTAX_ERROR }
