-- The composition and the placeholders are resolved only in the analyzer.
SET enable_analyzer = 1;

-- The alias of an `ARRAY JOIN` expression binds a bare identifier inside the subquery, so the
-- `y` it defines is local and the composition does not have to substitute into the subquery.
SELECT arrayMap((x -> x + 1) | (y -> y + (SELECT max(y) FROM numbers(1) ARRAY JOIN [5] AS y)), [1]);
SELECT arrayMap((x -> x + 1) | (y -> y + (SELECT max(y) FROM numbers(1) ARRAY JOIN [5] AS z, [7] AS y)), [1]);

-- An alias on another name leaves a free `y`, which is still rejected.
SELECT arrayMap((x -> x + 1) | (y -> y + (SELECT max(y) FROM numbers(1) ARRAY JOIN [5] AS z)), [1]); -- { serverError NOT_IMPLEMENTED }
-- The binding applies only inside the subquery that defines it.
SELECT arrayMap((x -> x + 1) | (y -> y + (SELECT max(z) FROM numbers(1) ARRAY JOIN [5] AS z) + (SELECT max(y) FROM numbers(1) ARRAY JOIN [5] AS z)), [1]); -- { serverError NOT_IMPLEMENTED }
