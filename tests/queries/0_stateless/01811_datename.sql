SELECT dateName('year', toDateTime('2021-04-14 11:22:33'));
SELECT dateName('quarter', toDateTime('2021-04-14 11:22:33'));
SELECT dateName('month', toDateTime('2021-04-14 11:22:33'));
SELECT dateName('dayofyear', toDateTime('2021-04-14 11:22:33'));
SELECT dateName('day', toDateTime('2021-04-14 11:22:33'));
SELECT dateName('week', toDateTime('2021-04-14 11:22:33'));
SELECT dateName('weekday', toDateTime('2021-04-14 11:22:33'));
SELECT dateName('hour', toDateTime('2021-04-14 11:22:33'));
SELECT dateName('minute', toDateTime('2021-04-14 11:22:33'));
SELECT dateName('second', toDateTime('2021-04-14 11:22:33'));


SELECT dateName('year', toDateTime64('2021-04-14 11:22:33', 3));
SELECT dateName('quarter', toDateTime64('2021-04-14 11:22:33', 3));
SELECT dateName('month', toDateTime64('2021-04-14 11:22:33', 3));
SELECT dateName('dayofyear', toDateTime64('2021-04-14 11:22:33', 3));
SELECT dateName('day', toDateTime64('2021-04-14 11:22:33', 3));
SELECT dateName('week', toDateTime64('2021-04-14 11:22:33', 3));
SELECT dateName('weekday', toDateTime64('2021-04-14 11:22:33', 3));
SELECT dateName('hour', toDateTime64('2021-04-14 11:22:33', 3));
SELECT dateName('minute', toDateTime64('2021-04-14 11:22:33', 3));
SELECT dateName('second', toDateTime64('2021-04-14 11:22:33', 3));


SELECT dateName('year', toDate('2021-04-14'));
SELECT dateName('quarter', toDate('2021-04-14'));
SELECT dateName('month', toDate('2021-04-14'));
SELECT dateName('dayofyear', toDate('2021-04-14'));
SELECT dateName('day', toDate('2021-04-14'));
SELECT dateName('week', toDate('2021-04-14'));
SELECT dateName('weekday', toDate('2021-04-14'));


SELECT dateName('hour', toDateTime('2021-04-14 11:22:33'), 'Europe/Moscow'),
       dateName('hour', toDateTime('2021-04-14 11:22:33'), 'UTC');

SELECT dateName('weekday', toDate('2021-04-12'));
SELECT dateName('weekday', toDate('2021-04-13'));
SELECT dateName('weekday', toDate('2021-04-14'));
SELECT dateName('weekday', toDate('2021-04-15'));
SELECT dateName('weekday', toDate('2021-04-16'));
SELECT dateName('weekday', toDate('2021-04-17'));
SELECT dateName('weekday', toDate('2021-04-18'));

SELECT dateName('month', toDate('2021-01-14'));
SELECT dateName('month', toDate('2021-02-14'));
SELECT dateName('month', toDate('2021-03-14'));
SELECT dateName('month', toDate('2021-04-14'));
SELECT dateName('month', toDate('2021-05-14'));
SELECT dateName('month', toDate('2021-06-14'));
SELECT dateName('month', toDate('2021-07-14'));
SELECT dateName('month', toDate('2021-08-14'));
SELECT dateName('month', toDate('2021-09-14'));
SELECT dateName('month', toDate('2021-10-14'));
SELECT dateName('month', toDate('2021-11-14'));
SELECT dateName('month', toDate('2021-12-14'));
