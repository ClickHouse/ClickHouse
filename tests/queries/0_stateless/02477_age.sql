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
