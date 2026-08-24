SELECT DISTINCT number % 3, number % 5, (number % 3, number % 5), [number % 3, number % 5] FROM (SELECT * FROM system.numbers LIMIT 100);
SELECT count(), count(DISTINCT x, y) FROM (SELECT DISTINCT * FROM (SELECT 'a\0b' AS x, 'c' AS y UNION ALL SELECT 'a', 'b\0c'));
SELECT count(), count(DISTINCT x, y) FROM (SELECT DISTINCT * FROM (SELECT [1, 2] AS x, [3] AS y UNION ALL SELECT [1], [2, 3]));
