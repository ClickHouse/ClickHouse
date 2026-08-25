-- `ParserProjectionSelectQuery` stores projection `ORDER BY` either as a single expression or as
-- a `tuple(...)` function whose arguments carry the comma-separated keys — never as a bare
-- `ASTExpressionList` or a sort-wrapper node. A parser-impossible shape in this slot would format
-- as an ordinary `ORDER BY a, b` but later fail in projection analysis when `cloneToASTSelect`
-- splices the node into the synthetic `SELECT` list, so `readJSON` must reject it at the boundary.

-- Parser-produced shapes round-trip byte-identically.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt64, PROJECTION p (SELECT a ORDER BY a)) ENGINE = MergeTree ORDER BY a'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt64, b UInt64, PROJECTION p (SELECT a, b ORDER BY a, b)) ENGINE = MergeTree ORDER BY a'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt64, PROJECTION p (SELECT a ORDER BY a + 1)) ENGINE = MergeTree ORDER BY a'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t ADD PROJECTION p (SELECT a, b ORDER BY b, a)'));

-- A bare `ExpressionList` in the `order_by` slot is parser-impossible.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (a UInt64, PROJECTION p (SELECT a ORDER BY a)) ENGINE = MergeTree ORDER BY a'),
    '"order_by":{"type":"Identifier","name":"a"}',
    '"order_by":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"}]}')); -- { serverError BAD_ARGUMENTS }

-- Sort-wrapper nodes (`OrderByElement`, `StorageOrderByElement`) never appear in this slot either.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (a UInt64, PROJECTION p (SELECT a ORDER BY a)) ENGINE = MergeTree ORDER BY a'),
    '"order_by":{"type":"Identifier","name":"a"}',
    '"order_by":{"type":"OrderByElement","direction":1,"nulls_direction":1,"expression":{"type":"Identifier","name":"a"}}')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (a UInt64, PROJECTION p (SELECT a ORDER BY a)) ENGINE = MergeTree ORDER BY a'),
    '"order_by":{"type":"Identifier","name":"a"}',
    '"order_by":{"type":"StorageOrderByElement","direction":1,"children":[{"type":"Identifier","name":"a"}]}')); -- { serverError BAD_ARGUMENTS }
