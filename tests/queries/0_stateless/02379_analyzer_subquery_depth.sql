SET enable_analyzer = 1;

SELECT (SELECT a FROM (SELECT 1 AS a)) SETTINGS max_subquery_depth = 1; -- { serverError TOO_DEEP_SUBQUERIES }
SELECT (SELECT a FROM (SELECT 1 AS a)) SETTINGS max_subquery_depth = 2;
