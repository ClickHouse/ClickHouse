-- Placeholders are not looked for inside subqueries: an identifier there is resolved as usual,
-- like the argument of an explicit lambda, which a subquery cannot reference either.

SET enable_analyzer = 1;
SET allow_correlated_subqueries = 1;

-- A subquery alone is not a placeholder expression, so `_1` in it is an ordinary identifier.
SELECT arrayMap((SELECT _1), [1]); -- { serverError UNKNOWN_IDENTIFIER }

-- A subquery next to a placeholder does not take part in the lifting.
SELECT arrayMap(plus(_1, (SELECT 10)), [1, 2]);

-- A name `_1` that the subquery binds itself refers to that binding.
SELECT arrayMap(_1 + (SELECT _1 FROM (SELECT 10 AS _1)), [1, 2]);
SELECT arrayMap(plus(_, 1) | multiply(_, (SELECT _1 FROM (SELECT 3 AS _1))), [1, 2]);
