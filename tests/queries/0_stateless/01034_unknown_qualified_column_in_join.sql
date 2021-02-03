SELECT l.c FROM (SELECT 1 AS a, 2 AS b) AS l join (SELECT 2 AS b, 3 AS c) AS r USING b; -- { serverError 47 }
SELECT r.a FROM (SELECT 1 AS a, 2 AS b) AS l join (SELECT 2 AS b, 3 AS c) AS r USING b; -- { serverError 47 }
SELECT l.a, r.c FROM (SELECT 1 AS a, 2 AS b) AS l join (SELECT 2 AS b, 3 AS c) AS r USING b;
