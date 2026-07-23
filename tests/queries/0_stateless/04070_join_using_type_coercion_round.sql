-- `round(y, x)` in WHERE on a `JOIN USING` column must not cause a type mismatch exception.
SELECT * FROM (SELECT 1 AS x, 1 AS y) AS a JOIN (SELECT 65537 AS y) AS b USING (y) WHERE round(y, x) = b.y;
