-- The internal function `__actionName` takes the action name of its result from the second
-- argument. Its first argument may be an arbitrary expression of an arbitrary type, so a direct
-- call can request a name that also belongs to something else the query computes. Requesting the
-- name of an input column must not shadow that column: column action names in the plan are column
-- identifiers (`__table1.number`), and an explicit alias of the query is not an action name at
-- all, so neither can be reused by name.

SET enable_analyzer = 1;

-- The name of an input column, in both visiting orders.
SELECT __actionName(number + 1, 'number') FROM numbers(3);
SELECT __actionName(number + 1, 'number'), number FROM numbers(3);
SELECT number, __actionName(number + 1, 'number') FROM numbers(3);

-- The name of an explicit alias of the same query.
SELECT __actionName(number + 1, 'x'), number + 100 AS x FROM numbers(3);

-- Inside an aggregate function, where a collapsed node would be visible in the result.
SELECT sum(__actionName(number + 1, 'number')), sum(number) FROM numbers(3);
