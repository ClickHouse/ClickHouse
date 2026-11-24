-- The composition and the placeholders are resolved only in the analyzer.
SET enable_analyzer = 1;

-- The operator syntax `f | g` always denotes a composition. A lambda bound in the query under
-- the name the operator parses into applies to an ordinary call only and does not change the
-- meaning of the operator.
WITH (x, y) -> x + y AS __compose SELECT 1 | 2; -- { serverError BAD_ARGUMENTS }
WITH (x, y) -> x + y AS __compose SELECT __compose(1, 2);
WITH (x, y) -> x + y AS __compose SELECT arrayMap(plus(_, 1) | multiply(_, 2), [1, 2, 3]);
WITH (x, y) -> x + y AS __compose SELECT arrayMap(__compose(_, 1) | multiply(_, 2), [1, 2, 3]);

-- A `UNION` exposes the column names of its first query only. A name that a later query
-- aliases is not local to the subquery that selects from the union, so a free occurrence of the
-- substituted name is still rejected, while a name the first query exposes is local.
SELECT arrayMap((x -> x + 1) | (y -> y + (SELECT max(y) FROM ((SELECT 1 AS z) UNION ALL SELECT 2 AS y) AS s)), [1]); -- { serverError NOT_IMPLEMENTED }
SELECT arrayMap((x -> x + 1) | (y -> y + (SELECT max(y) FROM ((SELECT 1 AS y) UNION ALL SELECT 2 AS z) AS s)), [1]);
SELECT arrayMap((x -> x + 1) | (y -> y + (SELECT max(y) FROM (SELECT 1 AS y UNION ALL SELECT 2) AS s)), [1]);
