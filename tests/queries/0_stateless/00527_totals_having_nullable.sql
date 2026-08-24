SELECT count() AS x WITH TOTALS HAVING x != toNullable(0);
SELECT k, count() AS c FROM (SELECT number, CASE WHEN number < 10 THEN 'hello' WHEN number < 50 THEN 'world' ELSE 'goodbye' END AS k FROM system.numbers LIMIT 100) GROUP BY k WITH TOTALS HAVING nullIf(c, 10) < 50 ORDER BY c;
