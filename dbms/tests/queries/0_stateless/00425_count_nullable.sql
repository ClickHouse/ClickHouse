SELECT count() FROM (SELECT number AS x FROM system.numbers LIMIT 10);

SELECT count(x) FROM (SELECT number AS x FROM system.numbers LIMIT 10);
SELECT count(x, y) FROM (SELECT number AS x, number AS y FROM system.numbers LIMIT 10);

SELECT count(x) FROM (SELECT CAST(number AS Nullable(UInt64)) AS x FROM system.numbers LIMIT 10);
SELECT count(x, y) FROM (SELECT CAST(number AS Nullable(UInt64)) AS x, number AS y FROM system.numbers LIMIT 10);
SELECT count(x, y) FROM (SELECT number AS x, CAST(number AS Nullable(UInt64)) AS y FROM system.numbers LIMIT 10);
SELECT count(x, y) FROM (SELECT CAST(number AS Nullable(UInt64)) AS x, CAST(number AS Nullable(UInt64)) AS y FROM system.numbers LIMIT 10);

SELECT count(x) FROM (SELECT nullIf(number, 5) AS x FROM system.numbers LIMIT 10);
SELECT count(x, y) FROM (SELECT nullIf(number, 5) AS x, number AS y FROM system.numbers LIMIT 10);
SELECT count(x, y) FROM (SELECT number AS x, nullIf(number, 3) AS y FROM system.numbers LIMIT 10);
SELECT count(x, y) FROM (SELECT nullIf(number, 5) AS x, nullIf(number, 3) AS y FROM system.numbers LIMIT 10);

SELECT count(NULL);
