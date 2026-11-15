SELECT 'Various intervals';

SELECT age('year', toDate('2017-12-31'), toDate('2016-01-01'));
SELECT age('year', toDate('2017-12-31'), toDate('2017-01-01'));
SELECT age('year', toDate('2017-12-31'), toDate('2018-01-01'));
SELECT age('quarter', toDate('2017-12-31'), toDate('2016-01-01'));
SELECT age('quarter', toDate('2017-12-31'), toDate('2017-01-01'));
SELECT age('quarter', toDate('2017-12-31'), toDate('2018-01-01'));
SELECT age('month', toDate('2017-12-31'), toDate('2016-01-01'));
SELECT age('month', toDate('2017-12-31'), toDate('2017-01-01'));
SELECT age('month', toDate('2017-12-31'), toDate('2018-01-01'));
SELECT age('week', toDate('2017-12-31'), toDate('2016-01-01'));
SELECT age('week', toDate('2017-12-31'), toDate('2017-01-01'));
SELECT age('week', toDate('2017-12-31'), toDate('2018-01-01'));
SELECT age('day', toDate('2017-12-31'), toDate('2016-01-01'));
SELECT age('day', toDate('2017-12-31'), toDate('2017-01-01'));
SELECT age('day', toDate('2017-12-31'), toDate('2018-01-01'));
SELECT age('hour', toDate('2017-12-31'), toDate('2016-01-01'), 'UTC');
SELECT age('hour', toDate('2017-12-31'), toDate('2017-01-01'), 'UTC');
SELECT age('hour', toDate('2017-12-31'), toDate('2018-01-01'), 'UTC');
SELECT age('minute', toDate('2017-12-31'), toDate('2016-01-01'), 'UTC');
SELECT age('minute', toDate('2017-12-31'), toDate('2017-01-01'), 'UTC');
SELECT age('minute', toDate('2017-12-31'), toDate('2018-01-01'), 'UTC');
SELECT age('second', toDate('2017-12-31'), toDate('2016-01-01'), 'UTC');
SELECT age('second', toDate('2017-12-31'), toDate('2017-01-01'), 'UTC');
SELECT age('second', toDate('2017-12-31'), toDate('2018-01-01'), 'UTC');

SELECT 'DateTime arguments';
SELECT age('day', toDateTime('2016-01-01 00:00:01', 'UTC'), toDateTime('2016-01-02 00:00:00', 'UTC'), 'UTC');
SELECT age('hour', toDateTime('2016-01-01 00:00:01', 'UTC'), toDateTime('2016-01-02 00:00:00', 'UTC'), 'UTC');
SELECT age('minute', toDateTime('2016-01-01 00:00:01', 'UTC'), toDateTime('2016-01-02 00:00:00', 'UTC'), 'UTC');
SELECT age('second', toDateTime('2016-01-01 00:00:01', 'UTC'), toDateTime('2016-01-02 00:00:00', 'UTC'), 'UTC');

SELECT 'Date and DateTime arguments';

SELECT age('second', toDate('2017-12-31'), toDateTime('2016-01-01 00:00:00', 'UTC'), 'UTC');
SELECT age('second', toDateTime('2017-12-31 00:00:00', 'UTC'), toDate('2017-01-01'), 'UTC');
SELECT age('second', toDateTime('2017-12-31 00:00:00', 'UTC'), toDateTime('2018-01-01 00:00:00', 'UTC'));

SELECT 'Constant and non-constant arguments';

SELECT age('minute', materialize(toDate('2017-12-31')), toDate('2016-01-01'), 'UTC');
SELECT age('minute', toDate('2017-12-31'), materialize(toDate('2017-01-01')), 'UTC');
SELECT age('minute', materialize(toDate('2017-12-31')), materialize(toDate('2018-01-01')), 'UTC');

SELECT 'Case insensitive';

SELECT age('YeAr', toDate('2017-12-31'), toDate('2016-01-01'));

SELECT 'Dependance of timezones';

SELECT age('month', toDate('2014-10-26'), toDate('2014-10-27'), 'Asia/Istanbul');
SELECT age('week', toDate('2014-10-26'), toDate('2014-10-27'), 'Asia/Istanbul');
SELECT age('day', toDate('2014-10-26'), toDate('2014-10-27'), 'Asia/Istanbul');
SELECT age('hour', toDate('2014-10-26'), toDate('2014-10-27'), 'Asia/Istanbul');
SELECT age('minute', toDate('2014-10-26'), toDate('2014-10-27'), 'Asia/Istanbul');
SELECT age('second', toDate('2014-10-26'), toDate('2014-10-27'), 'Asia/Istanbul');

SELECT age('month', toDate('2014-10-26'), toDate('2014-10-27'), 'UTC');
SELECT age('week', toDate('2014-10-26'), toDate('2014-10-27'), 'UTC');
SELECT age('day', toDate('2014-10-26'), toDate('2014-10-27'), 'UTC');
SELECT age('hour', toDate('2014-10-26'), toDate('2014-10-27'), 'UTC');
SELECT age('minute', toDate('2014-10-26'), toDate('2014-10-27'), 'UTC');
SELECT age('second', toDate('2014-10-26'), toDate('2014-10-27'), 'UTC');

SELECT age('month', toDateTime('2014-10-26 00:00:00', 'Asia/Istanbul'), toDateTime('2014-10-27 00:00:00', 'Asia/Istanbul'));
SELECT age('week', toDateTime('2014-10-26 00:00:00', 'Asia/Istanbul'), toDateTime('2014-10-27 00:00:00', 'Asia/Istanbul'));
SELECT age('day', toDateTime('2014-10-26 00:00:00', 'Asia/Istanbul'), toDateTime('2014-10-27 00:00:00', 'Asia/Istanbul'));
SELECT age('hour', toDateTime('2014-10-26 00:00:00', 'Asia/Istanbul'), toDateTime('2014-10-27 00:00:00', 'Asia/Istanbul'));
SELECT age('minute', toDateTime('2014-10-26 00:00:00', 'Asia/Istanbul'), toDateTime('2014-10-27 00:00:00', 'Asia/Istanbul'));
SELECT age('second', toDateTime('2014-10-26 00:00:00', 'Asia/Istanbul'), toDateTime('2014-10-27 00:00:00', 'Asia/Istanbul'));

SELECT age('month', toDateTime('2014-10-26 00:00:00', 'UTC'), toDateTime('2014-10-27 00:00:00', 'UTC'));
SELECT age('week', toDateTime('2014-10-26 00:00:00', 'UTC'), toDateTime('2014-10-27 00:00:00', 'UTC'));
SELECT age('day', toDateTime('2014-10-26 00:00:00', 'UTC'), toDateTime('2014-10-27 00:00:00', 'UTC'));
SELECT age('hour', toDateTime('2014-10-26 00:00:00', 'UTC'), toDateTime('2014-10-27 00:00:00', 'UTC'));
SELECT age('minute', toDateTime('2014-10-26 00:00:00', 'UTC'), toDateTime('2014-10-27 00:00:00', 'UTC'));
SELECT age('second', toDateTime('2014-10-26 00:00:00', 'UTC'), toDateTime('2014-10-27 00:00:00', 'UTC'));

SELECT 'Additional test';

SELECT number = age('month', now() - INTERVAL number MONTH, now()) FROM system.numbers LIMIT 10;

SELECT 'Full weeks';

SELECT age('week', toDateTime('2024-01-01 10:50:00', 'UTC'), toDateTime('2024-01-10 10:40:00', 'UTC'));
SELECT age('week', toDateTime('2024-01-10 10:40:00', 'UTC'), toDateTime('2024-01-01 10:50:00', 'UTC'));
SELECT age('week', toDate('2024-01-01'), toDate('2024-01-10'));
SELECT age('week', toDate('2024-01-10'), toDate('2024-01-01'));
SELECT age('week', toDate('2024-01-03'), toDate('2024-01-08'));
SELECT age('week', toDate('2024-01-08'), toDate('2024-01-03'));

-- In UTC every week is 604800 seconds long, so the result must be the elapsed time divided by a week, rounded towards zero.
SELECT count(), countIf(age('week', a, b) != intDiv(toUnixTimestamp64Nano(b) - toUnixTimestamp64Nano(a), 604800000000000))
FROM
(
    SELECT
        toDateTime64('2024-01-01 00:00:00', 9, 'UTC') + toIntervalNanosecond(cityHash64(number) % (21 * 86400000000000)) AS a,
        a + toIntervalNanosecond(toInt64(k) * 604800000000000 + delta) AS b
    FROM numbers(1000)
    ARRAY JOIN [-2, -1, 0, 1, 2] AS k
    ARRAY JOIN [-86400000000000, -3600000000000, -60000000000, -1000000000, -1000000, -1000, -1, 0,
                1, 1000, 1000000, 1000000000, 60000000000, 3600000000000, 86400000000000] AS delta
);

SELECT count(), countIf(age('week', a, b) != intDiv(toInt64(toUnixTimestamp(b)) - toUnixTimestamp(a), 604800))
FROM
(
    SELECT
        toDateTime('2024-01-01 00:00:00', 'UTC') + toIntervalSecond(cityHash64(number, 1) % (35 * 86400)) AS a,
        toDateTime('2024-01-01 00:00:00', 'UTC') + toIntervalSecond(cityHash64(number, 2) % (35 * 86400)) AS b
    FROM numbers(10000)
);

SELECT count(), countIf(age('week', a, b) != intDiv(dateDiff('day', a, b), 7))
FROM
(
    SELECT toDate('2024-01-01') + intDiv(number, 30) AS a, toDate('2024-01-01') + number % 30 AS b
    FROM numbers(900)
);

SELECT count(), countIf(age('week', a, b, 'UTC') != intDiv(toUnixTimestamp64Milli(b) - toUnixTimestamp64Milli(toDateTime64(a, 3, 'UTC')), 604800000))
FROM
(
    SELECT
        toDate32('1960-01-01') + intDiv(number, 30) AS a,
        toDateTime64('1960-01-01 00:00:00', 3, 'UTC') + toIntervalMillisecond(cityHash64(number) % (30 * 86400000)) AS b
    FROM numbers(900)
);
