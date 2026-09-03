SET enable_analyzer=1;

SELECT v.x, r.a, sum(c)
FROM (select 1 x, 2 c) AS v
ANY LEFT JOIN (SELECT 1 x, 2 a) AS r ON v.x = r.x
GROUP BY v.x; -- { serverError NOT_AN_AGGREGATE}
