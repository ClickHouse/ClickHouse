DROP TABLE IF EXISTS test;

CREATE TABLE test (a DateTime, b DateTime(), c DateTime(2), d DateTime('Asia/Istanbul'), e DateTime(3, 'Asia/Istanbul'), f DateTime32, g DateTime32('Asia/Istanbul'), h DateTime(0)) ENGINE = MergeTree ORDER BY a;

INSERT INTO test VALUES('2020-01-01 00:00:00', '2020-01-01 00:01:00', '2020-01-01 00:02:00.11', '2020-01-01 00:03:00', '2020-01-01 00:04:00.22', '2020-01-01 00:05:00', '2020-01-01 00:06:00', '2020-01-01 00:06:00');

SELECT a, toTypeName(a), b, toTypeName(b), c, toTypeName(c), d, toTypeName(d), e, toTypeName(e), f, toTypeName(f), g, toTypeName(g), h, toTypeName(h) FROM test;

SELECT toDateTime('2020-01-01 00:00:00') AS a, toTypeName(a), toDateTime('2020-01-01 00:02:00.11', 2) AS b, toTypeName(b), toDateTime('2020-01-01 00:03:00', 'Asia/Istanbul') AS c, toTypeName(c), toDateTime('2020-01-01 00:04:00.22', 3, 'Asia/Istanbul') AS d, toTypeName(d), toDateTime('2020-01-01 00:05:00', 0) AS e, toTypeName(e);

SELECT CAST('2020-01-01 00:00:00', 'DateTime') AS a, toTypeName(a), CAST('2020-01-01 00:02:00.11', 'DateTime(2)') AS b, toTypeName(b), CAST('2020-01-01 00:03:00', 'DateTime(\'Asia/Istanbul\')') AS c, toTypeName(c), CAST('2020-01-01 00:04:00.22', 'DateTime(3, \'Asia/Istanbul\')') AS d, toTypeName(d), CAST('2020-01-01 00:05:00', 'DateTime(0)') AS e, toTypeName(e);

SELECT toDateTime32('2020-01-01 00:00:00') AS a, toTypeName(a);

SELECT 'parseDateTimeBestEffort';
SELECT parseDateTimeBestEffort('<Empty>', 3) AS a, toTypeName(a); -- {serverError CANNOT_PARSE_DATETIME}
SELECT parseDateTimeBestEffort('2020-05-14T03:37:03', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffort('2020-05-14 03:37:03', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffort('2020-05-14 11:37:03 AM', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffort('2020-05-14 11:37:03 PM', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffort('2020-05-14 12:37:03 AM', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffort('2020-05-14 12:37:03 PM', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffort('2020-05-14T03:37:03.253184', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffort('2020-05-14T03:37:03.253184Z', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffort('2020-05-14T03:37:03.253184Z', 3, 'Europe/Minsk') AS a, toTypeName(a);
SELECT parseDateTimeBestEffort(materialize('2020-05-14T03:37:03.253184Z'), 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffort('1640649600123', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffort('1640649600123', 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffort('Dec 15, 2021') AS a, toTypeName(a);
SELECT parseDateTimeBestEffort('Dec 15, 2021', 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffort('Dec 15, 2021', 3, 'UTC') AS a, toTypeName(a);

SELECT 'parseDateTimeBestEffortOrNull';
SELECT parseDateTimeBestEffortOrNull('<Empty>', 3) AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrNull('2020-05-14T03:37:03', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrNull('2020-05-14 03:37:03', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrNull('2020-05-14 11:37:03 AM', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrNull('2020-05-14 11:37:03 PM', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrNull('2020-05-14 12:37:03 AM', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrNull('2020-05-14 12:37:03 PM', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrNull('2020-05-14T03:37:03.253184', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrNull('2020-05-14T03:37:03.253184Z', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrNull('2020-05-14T03:37:03.253184Z', 3, 'Europe/Minsk') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrNull(materialize('2020-05-14T03:37:03.253184Z'), 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrNull('1640649600123', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrNull('1640649600123', 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrNull('Dec 15, 2021') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrNull('Dec 15, 2021', 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrNull('Dec 15, 2021', 3, 'UTC') AS a, toTypeName(a);

SELECT 'parseDateTimeBestEffortOrZero';
SELECT parseDateTimeBestEffortOrZero('<Empty>', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrZero('2020-05-14T03:37:03', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrZero('2020-05-14 03:37:03', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrZero('2020-05-14 11:37:03 AM', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrZero('2020-05-14 11:37:03 PM', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrZero('2020-05-14 12:37:03 AM', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrZero('2020-05-14 12:37:03 PM', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrZero('2020-05-14T03:37:03.253184', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrZero('2020-05-14T03:37:03.253184Z', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrZero('2020-05-14T03:37:03.253184Z', 3, 'Europe/Minsk') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrZero(materialize('2020-05-14T03:37:03.253184Z'), 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrZero('1640649600123', 3, 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrZero('1640649600123', 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrZero('Dec 15, 2021') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrZero('Dec 15, 2021', 'UTC') AS a, toTypeName(a);
SELECT parseDateTimeBestEffortOrZero('Dec 15, 2021', 3, 'UTC') AS a, toTypeName(a);

SELECT 'parseDateTime32BestEffort';
SELECT parseDateTime32BestEffort('<Empty>') AS a, toTypeName(a); -- {serverError CANNOT_PARSE_DATETIME}
SELECT parseDateTime32BestEffort('2020-05-14T03:37:03', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffort('2020-05-14 03:37:03', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffort('2020-05-14 11:37:03 AM', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffort('2020-05-14 11:37:03 PM', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffort('2020-05-14 12:37:03 AM', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffort('2020-05-14 12:37:03 PM', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffort('2020-05-14T03:37:03.253184', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffort('2020-05-14T03:37:03.253184Z', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffort('2020-05-14T03:37:03.253184Z', 'Europe/Minsk') AS a, toTypeName(a);
SELECT parseDateTime32BestEffort(materialize('2020-05-14T03:37:03.253184Z'), 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffort('1640649600123', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffort('Dec 15, 2021') AS a, toTypeName(a);
SELECT parseDateTime32BestEffort('Dec 15, 2021', 'UTC') AS a, toTypeName(a);

SELECT 'parseDateTime32BestEffortOrNull';
SELECT parseDateTime32BestEffortOrNull('<Empty>') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrNull('2020-05-14T03:37:03', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrNull('2020-05-14 03:37:03', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrNull('2020-05-14 11:37:03 AM', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrNull('2020-05-14 11:37:03 PM', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrNull('2020-05-14 12:37:03 AM', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrNull('2020-05-14 12:37:03 PM', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrNull('2020-05-14T03:37:03.253184', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrNull('2020-05-14T03:37:03.253184Z', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrNull('2020-05-14T03:37:03.253184Z', 'Europe/Minsk') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrNull(materialize('2020-05-14T03:37:03.253184Z'), 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrNull('1640649600123', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrNull('Dec 15, 2021') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrNull('Dec 15, 2021', 'UTC') AS a, toTypeName(a);

SELECT 'parseDateTime32BestEffortOrZero';
SELECT parseDateTime32BestEffortOrZero('<Empty>', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrZero('2020-05-14T03:37:03', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrZero('2020-05-14 03:37:03', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrZero('2020-05-14 11:37:03 AM', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrZero('2020-05-14 11:37:03 PM', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrZero('2020-05-14 12:37:03 AM', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrZero('2020-05-14 12:37:03 PM', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrZero('2020-05-14T03:37:03.253184', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrZero('2020-05-14T03:37:03.253184Z', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrZero('2020-05-14T03:37:03.253184Z', 'Europe/Minsk') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrZero(materialize('2020-05-14T03:37:03.253184Z'), 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrZero('1640649600123', 'UTC') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrZero('Dec 15, 2021') AS a, toTypeName(a);
SELECT parseDateTime32BestEffortOrZero('Dec 15, 2021', 'UTC') AS a, toTypeName(a);

DROP TABLE IF EXISTS test;
