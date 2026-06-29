SELECT *
FROM ( SELECT number AS a, number + 1 AS b FROM numbers(1) ) AS l
INNER JOIN ( SELECT number AS a FROM numbers(2) ) AS r ON (l.a = r.a) OR (l.b = r.a)
ORDER BY ALL;
