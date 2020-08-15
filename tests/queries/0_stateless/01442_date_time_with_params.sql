DROP TABLE IF EXISTS test;

CREATE TABLE test (a DateTime, b DateTime(), c DateTime(2), d DateTime('Europe/Moscow'), e DateTime(3, 'Europe/Moscow'), f DateTime32, g DateTime32('Europe/Moscow')) ENGINE = MergeTree ORDER BY a;

INSERT INTO test VALUES('2020-01-01 00:00:00', '2020-01-01 00:01:00', '2020-01-01 00:02:00.11', '2020-01-01 00:03:00', '2020-01-01 00:04:00.22', '2020-01-01 00:05:00', '2020-01-01 00:06:00')

SELECT a, toTypeName(a), b, toTypeName(b), c, toTypeName(c), d, toTypeName(d), e, toTypeName(e), f, toTypeName(f), g, toTypeName(g) FROM test;

SELECT toDateTime('2020-01-01 00:00:00') AS a, toTypeName(a), toDateTime('2020-01-01 00:02:00.11', 2) AS b, toTypeName(b), toDateTime('2020-01-01 00:03:00', 'Europe/Moscow') AS c, toTypeName(c), toDateTime('2020-01-01 00:04:00.22', 3, 'Europe/Moscow') AS d, toTypeName(d);

SELECT CAST('2020-01-01 00:00:00', 'DateTime') AS a, toTypeName(a), CAST('2020-01-01 00:02:00.11', 'DateTime(2)') AS b, toTypeName(b), CAST('2020-01-01 00:03:00', 'DateTime(\'Europe/Moscow\')') AS c, toTypeName(c), CAST('2020-01-01 00:04:00.22', 'DateTime(3, \'Europe/Moscow\')') AS d, toTypeName(d);

SELECT toDateTime32('2020-01-01 00:00:00') AS a, toTypeName(a);

DROP TABLE IF EXISTS test;
