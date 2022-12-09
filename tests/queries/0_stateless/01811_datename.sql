WITH
    toDate('2021-04-14') AS date_value,
    toDateTime('2021-04-14 11:22:33') AS date_time_value,
    toDateTime64('2021-04-14 11:22:33', 3) AS date_time_64_value
SELECT dateName('year', date_value), dateName('year', date_time_value), dateName('year', date_time_64_value);

WITH
    toDate('2021-04-14') AS date_value,
    toDateTime('2021-04-14 11:22:33') AS date_time_value,
    toDateTime64('2021-04-14 11:22:33', 3) AS date_time_64_value
SELECT dateName('quarter', date_value), dateName('quarter', date_time_value), dateName('quarter', date_time_64_value);

WITH
    toDate('2021-04-14') AS date_value,
    toDateTime('2021-04-14 11:22:33') AS date_time_value,
    toDateTime64('2021-04-14 11:22:33', 3) AS date_time_64_value
SELECT dateName('month', date_value), dateName('month', date_time_value), dateName('month', date_time_64_value);

WITH
    toDate('2021-04-14') AS date_value,
    toDateTime('2021-04-14 11:22:33') AS date_time_value,
    toDateTime64('2021-04-14 11:22:33', 3) AS date_time_64_value
SELECT dateName('dayofyear', date_value), dateName('dayofyear', date_time_value), dateName('dayofyear', date_time_64_value);

WITH
    toDate('2021-04-14') AS date_value,
    toDateTime('2021-04-14 11:22:33') AS date_time_value,
    toDateTime64('2021-04-14 11:22:33', 3) AS date_time_64_value
SELECT dateName('day', date_value), dateName('day', date_time_value), dateName('day', date_time_64_value);

WITH
    toDate('2021-04-14') AS date_value,
    toDateTime('2021-04-14 11:22:33') AS date_time_value,
    toDateTime64('2021-04-14 11:22:33', 3) AS date_time_64_value
SELECT dateName('week', date_value), dateName('week', date_time_value), dateName('week', date_time_64_value);

WITH
    toDate('2021-04-14') AS date_value,
    toDateTime('2021-04-14 11:22:33') AS date_time_value,
    toDateTime64('2021-04-14 11:22:33', 3) AS date_time_64_value
SELECT dateName('weekday', date_value), dateName('weekday', date_time_value), dateName('weekday', date_time_64_value);

WITH
    toDateTime('2021-04-14 11:22:33') AS date_time_value,
    toDateTime64('2021-04-14 11:22:33', 3) AS date_time_64_value
SELECT dateName('hour', date_time_value), dateName('hour', date_time_64_value);

WITH
    toDateTime('2021-04-14 11:22:33') AS date_time_value,
    toDateTime64('2021-04-14 11:22:33', 3) AS date_time_64_value
SELECT dateName('minute', date_time_value), dateName('minute', date_time_64_value);

WITH
    toDateTime('2021-04-14 11:22:33') AS date_time_value,
    toDateTime64('2021-04-14 11:22:33', 3) AS date_time_64_value
SELECT dateName('second', date_time_value), dateName('second', date_time_64_value);

WITH
    toDateTime('2021-04-14 23:22:33', 'UTC') as date
SELECT
    dateName('weekday', date, 'UTC'),
    dateName('hour', date, 'UTC'),
    dateName('minute', date, 'UTC'),
    dateName('second', date, 'UTC');

WITH
    toDateTime('2021-04-14 23:22:33', 'UTC') as date
SELECT
    dateName('weekday', date, 'Asia/Istanbul'),
    dateName('hour', date, 'Asia/Istanbul'),
    dateName('minute', date, 'Asia/Istanbul'),
    dateName('second', date, 'Asia/Istanbul');
