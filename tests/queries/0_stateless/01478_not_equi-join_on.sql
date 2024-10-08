SELECT * FROM (SELECT NULL AS a, 1 AS b) AS foo
RIGHT JOIN (SELECT 1024 AS b) AS bar
ON 1 = foo.b; -- { serverError 403 }

SELECT * FROM (SELECT NULL AS a, 1 AS b) AS foo
RIGHT JOIN (SELECT 1024 AS b) AS bar
ON 1 = bar.b; -- { serverError 403 }
