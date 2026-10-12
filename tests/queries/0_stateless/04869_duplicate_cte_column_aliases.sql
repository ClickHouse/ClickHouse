-- Duplicate names in a column alias list must be rejected instead of silently collapsing columns.
SET enable_analyzer = 1;

WITH t (a, a) AS (SELECT number, number + 10 FROM numbers(3)) SELECT * FROM t ORDER BY 1; -- { serverError BAD_ARGUMENTS }
WITH t (a, a) AS (SELECT number, number + 10 FROM numbers(3) UNION ALL SELECT 5, 6) SELECT * FROM t ORDER BY 1; -- { serverError BAD_ARGUMENTS }
WITH t (a, b, a) AS (SELECT 1, 2, 3) SELECT * FROM t; -- { serverError BAD_ARGUMENTS }
WITH t (`a`, a) AS (SELECT 1, 2) SELECT * FROM t; -- { serverError BAD_ARGUMENTS }
WITH t (a, a) AS (SELECT number, number + 10 FROM numbers(3) EXCEPT SELECT 1, 11) SELECT * FROM t ORDER BY 1; -- { serverError BAD_ARGUMENTS }
WITH t (a, a) AS (SELECT 1, 2 INTERSECT SELECT 1, 2) SELECT * FROM t ORDER BY 1; -- { serverError BAD_ARGUMENTS }
SELECT * FROM (SELECT number, number + 10 FROM numbers(3)) AS x (a, a) ORDER BY 1; -- { serverError BAD_ARGUMENTS }

-- Distinct names keep working, and column names are case sensitive.
WITH t (a, b) AS (SELECT number, number + 10 FROM numbers(3)) SELECT * FROM t ORDER BY 1;
WITH t (a, A) AS (SELECT 1, 2) SELECT a, A FROM t;
SELECT * FROM (SELECT number, number + 10 FROM numbers(3)) AS x (a, b) ORDER BY 1;
