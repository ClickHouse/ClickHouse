SELECT * FROM (SELECT * FROM (SELECT 0 AS a, toNullable(number) AS b, toString(number) AS c FROM numbers(1000000.)) ORDER BY a DESC, b DESC, c ASC LIMIT 1500) LIMIT 10;
