-- The composition and the placeholders are resolved only in the analyzer.
SET enable_analyzer = 1;
SET allow_correlated_subqueries = 1;

-- A column exposed by a subquery in the join tree binds the name in the query that selects from
-- it, but the expression behind the column may itself be an outer reference to the substituted
-- name. The composition cannot substitute into the subquery, so it is rejected.
SELECT arrayMap((x -> x + 1) | (y -> y + (SELECT max(y) FROM (SELECT y) AS s)), [1]) FROM (SELECT 100 AS y); -- { serverError NOT_IMPLEMENTED }
SELECT arrayMap((x -> x + 1) | (y -> y + (SELECT max(y) FROM (SELECT y) AS s, (SELECT 1 AS z) AS t)), [1]) FROM (SELECT 100 AS y); -- { serverError NOT_IMPLEMENTED }
SELECT arrayMap((x -> x + 1) | (y -> y + (SELECT max(y) FROM (SELECT 1 AS z) AS t JOIN (SELECT y) AS s ON 1)), [1]) FROM (SELECT 100 AS y); -- { serverError NOT_IMPLEMENTED }
SELECT arrayMap((x -> x + 1) | (y -> y + (SELECT max(y) FROM (SELECT y UNION ALL SELECT 1) AS s)), [1]) FROM (SELECT 100 AS y); -- { serverError NOT_IMPLEMENTED }

-- A subquery that binds the name itself is local, so the composition works: `(1 + 1) + 5`.
SELECT arrayMap((x -> x + 1) | (y -> y + (SELECT max(y) FROM (SELECT 5 AS y) AS s)), [1]) FROM (SELECT 100 AS y);
SELECT arrayMap((x -> x + 1) | (y -> y + (SELECT max(y) FROM (SELECT z AS y FROM (SELECT 5 AS z)) AS s)), [1]) FROM (SELECT 100 AS y);
