SELECT toString((1, 'Hello', toDate('2016-01-01'))), toString([1, 2, 3]);
SELECT (number, toString(number), range(number)) AS x, toString(x) FROM system.numbers LIMIT 10;
SELECT hex(toString(countState())) FROM (SELECT * FROM system.numbers LIMIT 10);

SELECT CAST((1, 'Hello', toDate('2016-01-01')) AS String), CAST([1, 2, 3] AS String);
SELECT (number, toString(number), range(number)) AS x, CAST(x AS String) FROM system.numbers LIMIT 10;
SELECT hex(CAST(countState() AS String)) FROM (SELECT * FROM system.numbers LIMIT 10);
