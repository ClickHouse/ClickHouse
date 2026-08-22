
SELECT 1, b
FROM numbers(1)
ARRAY JOIN [materialize(3) IN (SELECT 42)] AS b
;

SELECT *
FROM (SELECT materialize(42) as a) as t1
JOIN (SELECT materialize(1) as a) as t2
ON t1.a IN (SELECT 42) = t2.a
;
