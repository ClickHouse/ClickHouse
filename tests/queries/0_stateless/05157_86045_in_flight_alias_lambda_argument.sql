-- An alias whose expression is being resolved right now is not a binding that a nested aliased
-- expression can use, so it must not hide the argument of the lambda the alias is referenced from.

-- The only outer `x` is the projection alias that is being resolved, so `x` inside `issue` keeps
-- referring to the argument of the lambda: `[0]`.
WITH isNull(x) AS issue SELECT arrayMap(x -> issue, [1]) AS x;

-- The same shape with the alias written after the lambda: `x` and `issue` form a cycle of aliases.
SELECT arrayMap(x -> issue, [1]) AS x, isNull(x) AS issue; -- { serverError CYCLIC_ALIASES }

-- A control without any name collision: the error does not come from hiding lambda arguments.
SELECT arrayMap(y -> issue, [1]) AS x, isNull(x) AS issue; -- { serverError CYCLIC_ALIASES }

-- An outer binding that is not in flight still hides the argument: `x` inside `issue` is the
-- column of the table expression, not the argument of the lambda.
SELECT arrayMap(x -> issue, [1]) AS y, isNull(x) AS issue FROM (SELECT 1 AS x);
