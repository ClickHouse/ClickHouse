SELECT sum(DISTINCT x) FROM (SELECT number AS x FROM system.numbers LIMIT 1000);
SELECT sum(DISTINCT x) FROM (SELECT number % 13 AS x FROM system.numbers LIMIT 1000);
SELECT groupArray(DISTINCT x) FROM (SELECT number % 13 AS x FROM system.numbers LIMIT 1000);
SELECT corrStableDistinct(DISTINCT x, y) FROM (SELECT number % 11 AS x, number % 13 AS y FROM system.numbers LIMIT 1000);