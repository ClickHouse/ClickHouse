-- Scoping of the function composition operator `f | g`: the arguments of the fused lambda must
-- not capture anything an operand references, and the guard against substituting into a
-- subquery must respect nested scopes.

SET enable_analyzer = 1;

-- A column that happens to be named like a synthesized argument of the fused lambda is still
-- captured from the outside: `10 + (1 + 1)`.
SELECT arrayMap((x -> x + 1) | (y -> y + __composed_arg_1), [1]) FROM (SELECT 10 AS __composed_arg_1);

-- The same for a column referenced from the left operand.
SELECT arrayMap((x -> x + __composed_arg_1) | (y -> y * 2), [1]) FROM (SELECT 10 AS __composed_arg_1);

-- A binder nested in a subquery shadows the name only inside its own scope, so a free
-- occurrence of the same name elsewhere in the subquery is still an outer reference and the
-- composition is rejected instead of resolving to something else.
SELECT arrayMap((x -> x + 1) | (x -> x + (SELECT arrayMap(y -> y, [1])[1] + x)), [1]); -- { serverError NOT_IMPLEMENTED }
SELECT arrayMap((x -> x + 1) | (x -> x + (SELECT arrayMap(x -> x, [1])[1] + x)), [1]); -- { serverError NOT_IMPLEMENTED }

-- A name bound by the subquery itself (an alias) is not an outer reference, so the composition
-- works: `(1 + 1) + 5`.
SELECT arrayMap((x -> x + 1) | (y -> y + (SELECT max(x) FROM (SELECT 5 AS x))), [1]);
