SELECT 'Various intervals';

SELECT dateDiff('year', toDate('2017-12-31'), toDate('2016-01-01'));
SELECT dateDiff('year', toDate('2017-12-31'), toDate('2017-01-01'));
SELECT dateDiff('year', toDate('2017-12-31'), toDate('2018-01-01'));
SELECT dateDiff('quarter', toDate('2017-12-31'), toDate('2016-01-01'));
SELECT dateDiff('quarter', toDate('2017-12-31'), toDate('2017-01-01'));
SELECT dateDiff('quarter', toDate('2017-12-31'), toDate('2018-01-01'));
SELECT dateDiff('month', toDate('2017-12-31'), toDate('2016-01-01'));
SELECT dateDiff('month', toDate('2017-12-31'), toDate('2017-01-01'));
SELECT dateDiff('month', toDate('2017-12-31'), toDate('2018-01-01'));
SELECT dateDiff('week', toDate('2017-12-31'), toDate('2016-01-01'));
SELECT dateDiff('week', toDate('2017-12-31'), toDate('2017-01-01'));
SELECT dateDiff('week', toDate('2017-12-31'), toDate('2018-01-01'));
SELECT dateDiff('day', toDate('2017-12-31'), toDate('2016-01-01'));
SELECT dateDiff('day', toDate('2017-12-31'), toDate('2017-01-01'));
SELECT dateDiff('day', toDate('2017-12-31'), toDate('2018-01-01'));
SELECT dateDiff('hour', toDate('2017-12-31'), toDate('2016-01-01'), 'UTC');
SELECT dateDiff('hour', toDate('2017-12-31'), toDate('2017-01-01'), 'UTC');
SELECT dateDiff('hour', toDate('2017-12-31'), toDate('2018-01-01'), 'UTC');
SELECT dateDiff('minute', toDate('2017-12-31'), toDate('2016-01-01'), 'UTC');
SELECT dateDiff('minute', toDate('2017-12-31'), toDate('2017-01-01'), 'UTC');
SELECT dateDiff('minute', toDate('2017-12-31'), toDate('2018-01-01'), 'UTC');
SELECT dateDiff('second', toDate('2017-12-31'), toDate('2016-01-01'), 'UTC');
SELECT dateDiff('second', toDate('2017-12-31'), toDate('2017-01-01'), 'UTC');
SELECT dateDiff('second', toDate('2017-12-31'), toDate('2018-01-01'), 'UTC');

SELECT 'Date and DateTime arguments';

SELECT dateDiff('second', toDate('2017-12-31'), toDateTime('2016-01-01 00:00:00', 'UTC'), 'UTC');
SELECT dateDiff('second', toDateTime('2017-12-31 00:00:00', 'UTC'), toDate('2017-01-01'), 'UTC');
SELECT dateDiff('second', toDateTime('2017-12-31 00:00:00', 'UTC'), toDateTime('2018-01-01 00:00:00', 'UTC'));

SELECT 'Constant and non-constant arguments';

SELECT dateDiff('minute', materialize(toDate('2017-12-31')), toDate('2016-01-01'), 'UTC');
SELECT dateDiff('minute', toDate('2017-12-31'), materialize(toDate('2017-01-01')), 'UTC');
SELECT dateDiff('minute', materialize(toDate('2017-12-31')), materialize(toDate('2018-01-01')), 'UTC');

SELECT 'Case insensitive';

SELECT DATEDIFF('year', today(), today() - INTERVAL 10 YEAR);

SELECT 'Dependance of timezones';

SELECT dateDiff('month', toDate('2014-10-26'), toDate('2014-10-27'), 'Europe/Moscow');
SELECT dateDiff('week', toDate('2014-10-26'), toDate('2014-10-27'), 'Europe/Moscow');
SELECT dateDiff('day', toDate('2014-10-26'), toDate('2014-10-27'), 'Europe/Moscow');
SELECT dateDiff('hour', toDate('2014-10-26'), toDate('2014-10-27'), 'Europe/Moscow');
SELECT dateDiff('minute', toDate('2014-10-26'), toDate('2014-10-27'), 'Europe/Moscow');
SELECT dateDiff('second', toDate('2014-10-26'), toDate('2014-10-27'), 'Europe/Moscow');

SELECT dateDiff('month', toDate('2014-10-26'), toDate('2014-10-27'), 'UTC');
SELECT dateDiff('week', toDate('2014-10-26'), toDate('2014-10-27'), 'UTC');
SELECT dateDiff('day', toDate('2014-10-26'), toDate('2014-10-27'), 'UTC');
SELECT dateDiff('hour', toDate('2014-10-26'), toDate('2014-10-27'), 'UTC');
SELECT dateDiff('minute', toDate('2014-10-26'), toDate('2014-10-27'), 'UTC');
SELECT dateDiff('second', toDate('2014-10-26'), toDate('2014-10-27'), 'UTC');

SELECT dateDiff('month', toDateTime('2014-10-26 00:00:00', 'Europe/Moscow'), toDateTime('2014-10-27 00:00:00', 'Europe/Moscow'));
SELECT dateDiff('week', toDateTime('2014-10-26 00:00:00', 'Europe/Moscow'), toDateTime('2014-10-27 00:00:00', 'Europe/Moscow'));
SELECT dateDiff('day', toDateTime('2014-10-26 00:00:00', 'Europe/Moscow'), toDateTime('2014-10-27 00:00:00', 'Europe/Moscow'));
SELECT dateDiff('hour', toDateTime('2014-10-26 00:00:00', 'Europe/Moscow'), toDateTime('2014-10-27 00:00:00', 'Europe/Moscow'));
SELECT dateDiff('minute', toDateTime('2014-10-26 00:00:00', 'Europe/Moscow'), toDateTime('2014-10-27 00:00:00', 'Europe/Moscow'));
SELECT dateDiff('second', toDateTime('2014-10-26 00:00:00', 'Europe/Moscow'), toDateTime('2014-10-27 00:00:00', 'Europe/Moscow'));

SELECT dateDiff('month', toDateTime('2014-10-26 00:00:00', 'UTC'), toDateTime('2014-10-27 00:00:00', 'UTC'));
SELECT dateDiff('week', toDateTime('2014-10-26 00:00:00', 'UTC'), toDateTime('2014-10-27 00:00:00', 'UTC'));
SELECT dateDiff('day', toDateTime('2014-10-26 00:00:00', 'UTC'), toDateTime('2014-10-27 00:00:00', 'UTC'));
SELECT dateDiff('hour', toDateTime('2014-10-26 00:00:00', 'UTC'), toDateTime('2014-10-27 00:00:00', 'UTC'));
SELECT dateDiff('minute', toDateTime('2014-10-26 00:00:00', 'UTC'), toDateTime('2014-10-27 00:00:00', 'UTC'));
SELECT dateDiff('second', toDateTime('2014-10-26 00:00:00', 'UTC'), toDateTime('2014-10-27 00:00:00', 'UTC'));

SELECT 'Additional test';

SELECT number = dateDiff('month', now() - INTERVAL number MONTH, now()) FROM system.numbers LIMIT 10;
