SELECT number % 10000 AS k, anyLastIf(1.0, 0) AS x FROM (SELECT * FROM system.numbers LIMIT 1000 UNION ALL SELECT * FROM system.numbers LIMIT 1000) GROUP BY k HAVING x != 0;
