SET session_timezone = 'UTC';

SELECT '-- The average of a set of intervals is an interval of the same unit';

DROP TABLE IF EXISTS requests;
CREATE TABLE requests
(
    req_id    Int64                NOT NULL,
    start_at  DateTime64(6, 'UTC') NOT NULL,
    duration  IntervalMillisecond  NOT NULL,
) ENGINE = MergeTree
  ORDER BY start_at;

INSERT INTO requests VALUES (1, '2026-01-01 00:00:00', 100), (2, '2026-01-01 00:00:01', 200), (3, '2026-01-01 00:00:02', 300);

SELECT avg(duration) AS avg_duration, toTypeName(avg_duration) FROM requests;

DROP TABLE requests;

SELECT '-- Every interval kind';
SELECT avg(toIntervalNanosecond(number)) AS a, toTypeName(a) FROM numbers(5);
SELECT avg(toIntervalMicrosecond(number)) AS a, toTypeName(a) FROM numbers(5);
SELECT avg(toIntervalMillisecond(number)) AS a, toTypeName(a) FROM numbers(5);
SELECT avg(toIntervalSecond(number)) AS a, toTypeName(a) FROM numbers(5);
SELECT avg(toIntervalMinute(number)) AS a, toTypeName(a) FROM numbers(5);
SELECT avg(toIntervalHour(number)) AS a, toTypeName(a) FROM numbers(5);
SELECT avg(toIntervalDay(number)) AS a, toTypeName(a) FROM numbers(5);
SELECT avg(toIntervalWeek(number)) AS a, toTypeName(a) FROM numbers(5);
SELECT avg(toIntervalMonth(number)) AS a, toTypeName(a) FROM numbers(5);
SELECT avg(toIntervalQuarter(number)) AS a, toTypeName(a) FROM numbers(5);
SELECT avg(toIntervalYear(number)) AS a, toTypeName(a) FROM numbers(5);

SELECT '-- A fractional average is rounded half to even, as for the date and time types';
SELECT avg(x) FROM (SELECT toIntervalSecond(100) AS x UNION ALL SELECT toIntervalSecond(101) AS x);
SELECT avg(x) FROM (SELECT toIntervalSecond(101) AS x UNION ALL SELECT toIntervalSecond(102) AS x);
SELECT avg(x) FROM (SELECT toIntervalSecond(-100) AS x UNION ALL SELECT toIntervalSecond(-101) AS x);
SELECT avg(x) FROM (SELECT toIntervalSecond(-101) AS x UNION ALL SELECT toIntervalSecond(-102) AS x);

SELECT '-- The division is exact above the range where Float64 holds every integer';
SELECT avg(x) FROM (SELECT toIntervalNanosecond(2305843009213693952) AS x UNION ALL SELECT toIntervalNanosecond(2305843009213693954) AS x);

SELECT '-- An empty input gives a zero-length interval, as an empty input gives the epoch for a date';
SELECT avg(x) AS a, toTypeName(a) FROM (SELECT toIntervalDay(number) AS x FROM numbers(0));

SELECT '-- A single row';
SELECT avg(toIntervalDay(7));

SELECT '-- Nullable';
SELECT avg(x) AS a, toTypeName(a) FROM (SELECT if(number % 2, NULL, toIntervalSecond(number * 10)) AS x FROM numbers(5));
SELECT avgOrNull(x) AS a, toTypeName(a) FROM (SELECT toIntervalSecond(number) AS x FROM numbers(0));

SELECT '-- Combinators';
SELECT avgIf(toIntervalSecond(number), number % 2 = 0) AS a, toTypeName(a) FROM numbers(10);
SELECT avgMerge(s) AS a, toTypeName(a) FROM (SELECT avgState(toIntervalMinute(number)) AS s FROM numbers(7));
SELECT avgMerge(CAST(unhex(hex(s)) AS AggregateFunction(avg, IntervalMinute))) AS a, toTypeName(a) FROM (SELECT avgState(toIntervalMinute(number)) AS s FROM numbers(7));
SELECT avgDistinct(toIntervalSecond(number % 3)) AS a, toTypeName(a) FROM numbers(30);
SELECT avgResample(0, 2, 1)(toIntervalDay(number), number % 2) AS a, toTypeName(a) FROM numbers(10);
SELECT avgArray([toIntervalHour(1), toIntervalHour(2), toIntervalHour(4)]) AS a, toTypeName(a);

SELECT '-- GROUP BY';
SELECT number % 3 AS k, avg(toIntervalHour(number)) AS a FROM numbers(10) GROUP BY k ORDER BY k;

SELECT '-- The average of an interval can be added to a date';
SELECT toDateTime('2026-01-01 00:00:00') + avg(toIntervalSecond(number)) FROM numbers(11);
