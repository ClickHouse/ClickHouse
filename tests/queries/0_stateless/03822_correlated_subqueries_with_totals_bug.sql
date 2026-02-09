SELECT (SELECT count(*) FROM (SELECT t0.c0)) AS a0 FROM (SELECT 1 AS c0 GROUP BY 1 WITH TOTALS) AS t0 SETTINGS allow_experimental_correlated_subqueries = 1;
