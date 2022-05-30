WITH t AS (SELECT 'a','b' UNION ALL SELECT 'c','d') SELECT * FROM t;
WITH t(c1, c2) AS (SELECT 'a','b' UNION ALL SELECT 'c','d') SELECT c1, c2 FROM t;
WITH t (c1, c2) AS (SELECT 'a','b' UNION ALL SELECT 'c','d') SELECT c1, c2 FROM t;
-- WITH t (c1, c2) AS (SELECT * FROM (SELECT number AS a FROM numbers(2)) AS t1 JOIN (SELECT number AS b FROM numbers(2)) AS t2 ON t1.a = t2.b) SELECT * FROM t; -- { serverError ATTEMPT_TO_READ_AFTER_EOF }
