SET session_timezone = 'UTC';

SELECT '-- The sum of a set of intervals is an interval of the same unit';

DROP TABLE IF EXISTS requests;
CREATE TABLE requests
(
    req_id    Int64                NOT NULL,
    start_at  DateTime64(6, 'UTC') NOT NULL,
    duration  IntervalMillisecond  NOT NULL,
) ENGINE = MergeTree
  ORDER BY start_at;

INSERT INTO requests VALUES (1, '2026-01-01 00:00:00', 100), (2, '2026-01-01 00:00:01', 200), (3, '2026-01-01 00:00:02', 300);

SELECT sum(duration) AS total_duration, toTypeName(total_duration) FROM requests;

DROP TABLE requests;

SELECT '-- Every interval kind';
SELECT sum(toIntervalNanosecond(number)) AS s, toTypeName(s) FROM numbers(5);
SELECT sum(toIntervalMicrosecond(number)) AS s, toTypeName(s) FROM numbers(5);
SELECT sum(toIntervalMillisecond(number)) AS s, toTypeName(s) FROM numbers(5);
SELECT sum(toIntervalSecond(number)) AS s, toTypeName(s) FROM numbers(5);
SELECT sum(toIntervalMinute(number)) AS s, toTypeName(s) FROM numbers(5);
SELECT sum(toIntervalHour(number)) AS s, toTypeName(s) FROM numbers(5);
SELECT sum(toIntervalDay(number)) AS s, toTypeName(s) FROM numbers(5);
SELECT sum(toIntervalWeek(number)) AS s, toTypeName(s) FROM numbers(5);
SELECT sum(toIntervalMonth(number)) AS s, toTypeName(s) FROM numbers(5);
SELECT sum(toIntervalQuarter(number)) AS s, toTypeName(s) FROM numbers(5);
SELECT sum(toIntervalYear(number)) AS s, toTypeName(s) FROM numbers(5);

SELECT '-- The sum is exact above the range where Float64 holds every integer';
SELECT sum(x) FROM (SELECT toIntervalNanosecond(9007199254740993) AS x UNION ALL SELECT toIntervalNanosecond(2) AS x);

SELECT '-- Negative intervals';
SELECT sum(toIntervalSecond(-number)) AS s, toTypeName(s) FROM numbers(5);

SELECT '-- An empty input gives a zero-length interval';
SELECT sum(x) AS s, toTypeName(s) FROM (SELECT toIntervalDay(number) AS x FROM numbers(0));

SELECT '-- A single row';
SELECT sum(toIntervalDay(7));

SELECT '-- Nullable';
SELECT sum(x) AS s, toTypeName(s) FROM (SELECT if(number % 2, NULL, toIntervalSecond(number * 10)) AS x FROM numbers(5));
SELECT sumOrNull(x) AS s, toTypeName(s) FROM (SELECT toIntervalSecond(number) AS x FROM numbers(0));

SELECT '-- Combinators';
SELECT sumIf(toIntervalSecond(number), number % 2 = 0) AS s, toTypeName(s) FROM numbers(10);
SELECT sumMerge(s) AS a, toTypeName(a) FROM (SELECT sumState(toIntervalMinute(number)) AS s FROM numbers(7));
SELECT sumMerge(CAST(unhex(hex(s)) AS AggregateFunction(sum, IntervalMinute))) AS a, toTypeName(a) FROM (SELECT sumState(toIntervalMinute(number)) AS s FROM numbers(7));
SELECT sumDistinct(toIntervalSecond(number % 3)) AS s, toTypeName(s) FROM numbers(30);
SELECT sumResample(0, 2, 1)(toIntervalDay(number), number % 2) AS s, toTypeName(s) FROM numbers(10);
SELECT sumArray([toIntervalHour(1), toIntervalHour(2), toIntervalHour(4)]) AS s, toTypeName(s);

SELECT '-- GROUP BY';
SELECT number % 3 AS k, sum(toIntervalHour(number)) AS s FROM numbers(10) GROUP BY k ORDER BY k;

SELECT '-- The sum of intervals can be added to a date';
SELECT toDateTime('2026-01-01 00:00:00') + sum(toIntervalSecond(number)) FROM numbers(11);

SELECT '-- sumWithOverflow and sumKahan keep the interval type as well';
SELECT sumWithOverflow(toIntervalSecond(number)) AS s, toTypeName(s) FROM numbers(5);
SELECT sumKahan(toIntervalSecond(number)) AS s, toTypeName(s) FROM numbers(5);

SELECT '-- Values of different units are brought to their common unit before the aggregation';
SELECT sum(x) AS s, toTypeName(s) FROM (SELECT toIntervalMinute(1) AS x UNION ALL SELECT toIntervalSecond(1) AS x);

SELECT '-- A month is not a fixed number of days, so months and days have no common unit';
SELECT sum(x) FROM (SELECT toIntervalDay(1) AS x UNION ALL SELECT toIntervalMonth(1) AS x); -- { serverError NO_COMMON_TYPE }
