SELECT number, number / 2 AS n, j1, j2 FROM system.numbers ANY LEFT JOIN (SELECT number / 3 AS n, number AS j1, 'Hello' AS j2 FROM system.numbers LIMIT 10) js2 USING n LIMIT 10
