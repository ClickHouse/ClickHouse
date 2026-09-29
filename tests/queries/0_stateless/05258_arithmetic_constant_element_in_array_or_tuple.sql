-- Arithmetic must not produce a constant inside an array or a tuple: an array operation whose element-wise
-- result is a constant (a date plus a tuple holding NULL is NULL), and a constant interval added to a
-- non-constant interval of another unit.

SELECT toString([toDateTime64(1, 3)] + [(2, NULL)]);
SELECT toString([toDateTime64(number, 3)] + [(2, NULL)]), toJSONString([toDateTime64(number, 3)] + [(2, NULL)]) FROM numbers(2);
SELECT arrayPushBack([toDateTime64(number, 3)] + [(2, NULL)], NULL), arrayDistinct([toDateTime64(number, 3)] + [(2, NULL)]) FROM numbers(2);
SELECT toString([toDate('2020-01-01') + number] - [(1, NULL)]), toString([(1, NULL)] + [toDateTime(number)]) FROM numbers(2);
SELECT toString([[toDateTime64(number, 3)]] + [[(2, NULL)]]) FROM numbers(2);
SELECT [toDateTime64(number, 3)] + [(2, NULL)] FROM numbers(1) UNION ALL SELECT materialize([NULL]);
SELECT arrayPartialShuffle([toDateTime64(1, 3)] + [(2, NULL)]) AS a GROUP BY a WITH TOTALS;

SELECT toIntervalDay(number) + INTERVAL 1 HOUR, toIntervalDay(number) - INTERVAL 1 HOUR FROM numbers(2);
SELECT (INTERVAL 1 DAY, INTERVAL 1 HOUR) + toIntervalHour(number), (INTERVAL 1 DAY, INTERVAL 1 MONTH) + toIntervalHour(number) FROM numbers(2);
SELECT toDateTime('2020-01-01 00:00:00', 'UTC') + (toIntervalDay(number) + INTERVAL 1 HOUR) FROM numbers(2);
