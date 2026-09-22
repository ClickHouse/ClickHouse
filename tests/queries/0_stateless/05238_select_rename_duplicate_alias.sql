SET enable_analyzer = 1;

SELECT a AS x, b AS x FROM (SELECT 1 AS a, 2 AS b); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT * RENAME a AS x, * RENAME b AS x FROM (SELECT 1 AS a, 2 AS b); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
