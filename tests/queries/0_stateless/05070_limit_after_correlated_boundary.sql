-- A `LIMIT AFTER`/`UNTIL` boundary that refers to a column of the outer query is a correlated expression.
-- Decorrelation does not support the range step and reports that instead of failing during execution
-- with a missing column. A plain `LIMIT` after a correlated filter is supported and returns the
-- per-group top row.
-- Correlated subqueries are resolved by the analyzer only.
SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

SELECT o.number, (SELECT i.number FROM numbers(5) AS i ORDER BY i.number LIMIT 1 AFTER i.number >= o.number) AS picked FROM numbers(3) AS o ORDER BY o.number; -- { serverError NOT_IMPLEMENTED }
SELECT o.number, (SELECT i.number FROM numbers(5) AS i ORDER BY i.number LIMIT UNTIL i.number >= o.number) AS picked FROM numbers(3) AS o ORDER BY o.number; -- { serverError NOT_IMPLEMENTED }
SELECT o.number, (SELECT i.number FROM numbers(5) AS i WHERE i.number >= o.number ORDER BY i.number LIMIT 1) AS picked FROM numbers(3) AS o ORDER BY o.number;
