-- { echo }

-- DateTime64 vs DateTime64 same scale
SELECT age('second', toDateTime64('1927-01-01 00:00:00', 0, 'UTC'), toDateTime64('1927-01-01 00:00:10', 0, 'UTC'));
SELECT age('second', toDateTime64('1927-01-01 00:00:00', 0, 'UTC'), toDateTime64('1927-01-01 00:10:00', 0, 'UTC'));
SELECT age('second', toDateTime64('1927-01-01 00:00:00', 0, 'UTC'), toDateTime64('1927-01-01 01:00:00', 0, 'UTC'));
SELECT age('second', toDateTime64('1927-01-01 00:00:00', 0, 'UTC'), toDateTime64('1927-01-01 01:10:10', 0, 'UTC'));

SELECT age('minute', toDateTime64('1927-01-01 00:00:00', 0, 'UTC'), toDateTime64('1927-01-01 00:10:00', 0, 'UTC'));
SELECT age('minute', toDateTime64('1927-01-01 00:00:00', 0, 'UTC'), toDateTime64('1927-01-01 10:00:00', 0, 'UTC'));

SELECT age('hour', toDateTime64('1927-01-01 00:00:00', 0, 'UTC'), toDateTime64('1927-01-01 10:00:00', 0, 'UTC'));

SELECT age('day', toDateTime64('1927-01-01 00:00:00', 0, 'UTC'), toDateTime64('1927-01-02 00:00:00', 0, 'UTC'));
SELECT age('month', toDateTime64('1927-01-01 00:00:00', 0, 'UTC'), toDateTime64('1927-02-01 00:00:00', 0, 'UTC'));
SELECT age('year', toDateTime64('1927-01-01 00:00:00', 0, 'UTC'), toDateTime64('1928-01-01 00:00:00', 0, 'UTC'));

-- DateTime64 vs DateTime64 different scale
SELECT age('second', toDateTime64('1927-01-01 00:00:00', 6, 'UTC'), toDateTime64('1927-01-01 00:00:10', 3, 'UTC'));
SELECT age('second', toDateTime64('1927-01-01 00:00:00', 6, 'UTC'), toDateTime64('1927-01-01 00:10:00', 3, 'UTC'));
SELECT age('second', toDateTime64('1927-01-01 00:00:00', 6, 'UTC'), toDateTime64('1927-01-01 01:00:00', 3, 'UTC'));
SELECT age('second', toDateTime64('1927-01-01 00:00:00', 6, 'UTC'), toDateTime64('1927-01-01 01:10:10', 3, 'UTC'));

SELECT age('minute', toDateTime64('1927-01-01 00:00:00', 6, 'UTC'), toDateTime64('1927-01-01 00:10:00', 3, 'UTC'));
SELECT age('minute', toDateTime64('1927-01-01 00:00:00', 6, 'UTC'), toDateTime64('1927-01-01 10:00:00', 3, 'UTC'));

SELECT age('hour', toDateTime64('1927-01-01 00:00:00', 6, 'UTC'), toDateTime64('1927-01-01 10:00:00', 3, 'UTC'));

SELECT age('day', toDateTime64('1927-01-01 00:00:00', 6, 'UTC'), toDateTime64('1927-01-02 00:00:00', 3, 'UTC'));
SELECT age('month', toDateTime64('1927-01-01 00:00:00', 6, 'UTC'), toDateTime64('1927-02-01 00:00:00', 3, 'UTC'));
SELECT age('year', toDateTime64('1927-01-01 00:00:00', 6, 'UTC'), toDateTime64('1928-01-01 00:00:00', 3, 'UTC'));

-- With DateTime
-- DateTime64 vs DateTime
SELECT age('second', toDateTime64('2015-08-18 00:00:00', 0, 'UTC'), toDateTime('2015-08-18 00:00:00', 'UTC'));
SELECT age('second', toDateTime64('2015-08-18 00:00:00', 0, 'UTC'), toDateTime('2015-08-18 00:00:10', 'UTC'));
SELECT age('second', toDateTime64('2015-08-18 00:00:00', 0, 'UTC'), toDateTime('2015-08-18 00:10:00', 'UTC'));
SELECT age('second', toDateTime64('2015-08-18 00:00:00', 0, 'UTC'), toDateTime('2015-08-18 01:00:00', 'UTC'));
SELECT age('second', toDateTime64('2015-08-18 00:00:00', 0, 'UTC'), toDateTime('2015-08-18 01:10:10', 'UTC'));

-- DateTime vs DateTime64
SELECT age('second', toDateTime('2015-08-18 00:00:00', 'UTC'), toDateTime64('2015-08-18 00:00:00', 3, 'UTC'));
SELECT age('second', toDateTime('2015-08-18 00:00:00', 'UTC'), toDateTime64('2015-08-18 00:00:10', 3, 'UTC'));
SELECT age('second', toDateTime('2015-08-18 00:00:00', 'UTC'), toDateTime64('2015-08-18 00:10:00', 3, 'UTC'));
SELECT age('second', toDateTime('2015-08-18 00:00:00', 'UTC'), toDateTime64('2015-08-18 01:00:00', 3, 'UTC'));
SELECT age('second', toDateTime('2015-08-18 00:00:00', 'UTC'), toDateTime64('2015-08-18 01:10:10', 3, 'UTC'));

-- With Date
-- DateTime64 vs Date
SELECT age('day', toDateTime64('2015-08-18 00:00:00', 0, 'UTC'), toDate('2015-08-19', 'UTC'));

-- Date vs DateTime64
SELECT age('day', toDate('2015-08-18', 'UTC'), toDateTime64('2015-08-19 00:00:00', 3, 'UTC'));

-- Same thing but const vs non-const columns
SELECT age('second', toDateTime64('1927-01-01 00:00:00', 0, 'UTC'), materialize(toDateTime64('1927-01-01 00:00:10', 0, 'UTC')));
SELECT age('second', toDateTime64('1927-01-01 00:00:00', 6, 'UTC'), materialize(toDateTime64('1927-01-01 00:00:10', 3, 'UTC')));
SELECT age('second', toDateTime64('2015-08-18 00:00:00', 0, 'UTC'), materialize(toDateTime('2015-08-18 00:00:10', 'UTC')));
SELECT age('second', toDateTime('2015-08-18 00:00:00', 'UTC'), materialize(toDateTime64('2015-08-18 00:00:10', 3, 'UTC')));
SELECT age('day', toDateTime64('2015-08-18 00:00:00', 0, 'UTC'), materialize(toDate('2015-08-19', 'UTC')));
SELECT age('day', toDate('2015-08-18', 'UTC'), materialize(toDateTime64('2015-08-19 00:00:00', 3, 'UTC')));

-- Same thing but non-const vs const columns
SELECT age('second', materialize(toDateTime64('1927-01-01 00:00:00', 0, 'UTC')), toDateTime64('1927-01-01 00:00:10', 0, 'UTC'));
SELECT age('second', materialize(toDateTime64('1927-01-01 00:00:00', 6, 'UTC')), toDateTime64('1927-01-01 00:00:10', 3, 'UTC'));
SELECT age('second', materialize(toDateTime64('2015-08-18 00:00:00', 0, 'UTC')), toDateTime('2015-08-18 00:00:10', 'UTC'));
SELECT age('second', materialize(toDateTime('2015-08-18 00:00:00', 'UTC')), toDateTime64('2015-08-18 00:00:10', 3, 'UTC'));
SELECT age('day', materialize(toDateTime64('2015-08-18 00:00:00', 0, 'UTC')), toDate('2015-08-19', 'UTC'));
SELECT age('day', materialize(toDate('2015-08-18', 'UTC')), toDateTime64('2015-08-19 00:00:00', 3, 'UTC'));

-- Same thing but non-const vs non-const columns
SELECT age('second', materialize(toDateTime64('1927-01-01 00:00:00', 0, 'UTC')), materialize(toDateTime64('1927-01-01 00:00:10', 0, 'UTC')));
SELECT age('second', materialize(toDateTime64('1927-01-01 00:00:00', 6, 'UTC')), materialize(toDateTime64('1927-01-01 00:00:10', 3, 'UTC')));
SELECT age('second', materialize(toDateTime64('2015-08-18 00:00:00', 0, 'UTC')), materialize(toDateTime('2015-08-18 00:00:10', 'UTC')));
SELECT age('second', materialize(toDateTime('2015-08-18 00:00:00', 'UTC')), materialize(toDateTime64('2015-08-18 00:00:10', 3, 'UTC')));
SELECT age('day', materialize(toDateTime64('2015-08-18 00:00:00', 0, 'UTC')), materialize(toDate('2015-08-19', 'UTC')));
SELECT age('day', materialize(toDate('2015-08-18', 'UTC')), materialize(toDateTime64('2015-08-19 00:00:00', 3, 'UTC')));

-- DateTime64 vs DateTime64 with fractional part
SELECT age('microsecond', toDateTime64('2015-08-18 20:30:36.100200005', 9, 'UTC'), toDateTime64('2015-08-18 20:30:41.200400005', 9, 'UTC'));
SELECT age('microsecond', toDateTime64('2015-08-18 20:30:36.100200005', 9, 'UTC'), toDateTime64('2015-08-18 20:30:41.200400004', 9, 'UTC'));

SELECT age('millisecond', toDateTime64('2015-08-18 20:30:36.450299', 6, 'UTC'), toDateTime64('2015-08-18 20:30:41.550299', 6, 'UTC'));
SELECT age('millisecond', toDateTime64('2015-08-18 20:30:36.450299', 6, 'UTC'), toDateTime64('2015-08-18 20:30:41.550298', 6, 'UTC'));

SELECT age('second', toDateTime64('2023-03-01 19:18:36.999003', 6, 'UTC'), toDateTime64('2023-03-01 19:18:41.999002', 6, 'UTC'));
SELECT age('second', toDateTime64('2023-03-01 19:18:36.999', 3, 'UTC'), toDateTime64('2023-03-01 19:18:41.001', 3, 'UTC'));

SELECT age('minute', toDateTime64('2015-01-01 20:30:36.200', 3, 'UTC'), toDateTime64('2015-01-01 20:35:36.300', 3, 'UTC'));
SELECT age('minute', toDateTime64('2015-01-01 20:30:36.200', 3, 'UTC'), toDateTime64('2015-01-01 20:35:36.100', 3, 'UTC'));
SELECT age('minute', toDateTime64('2015-01-01 20:30:36.200101', 6, 'UTC'), toDateTime64('2015-01-01 20:35:36.200100', 6, 'UTC'));

SELECT age('hour', toDateTime64('2015-01-01 20:30:36.200', 3, 'UTC'), toDateTime64('2015-01-01 23:30:36.200', 3, 'UTC'));
SELECT age('hour', toDateTime64('2015-01-01 20:31:36.200', 3, 'UTC'), toDateTime64('2015-01-01 23:30:36.200', 3, 'UTC'));
SELECT age('hour', toDateTime64('2015-01-01 20:30:37.200', 3, 'UTC'), toDateTime64('2015-01-01 23:30:36.200', 3, 'UTC'));
SELECT age('hour', toDateTime64('2015-01-01 20:30:36.300', 3, 'UTC'), toDateTime64('2015-01-01 23:30:36.200', 3, 'UTC'));
SELECT age('hour', toDateTime64('2015-01-01 20:30:36.200101', 6, 'UTC'), toDateTime64('2015-01-01 23:30:36.200100', 6, 'UTC'));

SELECT age('day', toDateTime64('2015-01-01 20:30:36.200', 3, 'UTC'), toDateTime64('2015-01-04 20:30:36.200', 3, 'UTC'));
SELECT age('day', toDateTime64('2015-01-01 20:30:36.200', 3, 'UTC'), toDateTime64('2015-01-04 19:30:36.200', 3, 'UTC'));
SELECT age('day', toDateTime64('2015-01-01 20:30:36.200', 3, 'UTC'), toDateTime64('2015-01-04 20:28:36.200', 3, 'UTC'));
SELECT age('day', toDateTime64('2015-01-01 20:30:36.200', 3, 'UTC'), toDateTime64('2015-01-04 20:30:35.200', 3, 'UTC'));
SELECT age('day', toDateTime64('2015-01-01 20:30:36.200', 3, 'UTC'), toDateTime64('2015-01-04 20:30:36.199', 3, 'UTC'));
SELECT age('day', toDateTime64('2015-01-01 20:30:36.200101', 6, 'UTC'), toDateTime64('2015-01-04 20:30:36.200100', 6, 'UTC'));

SELECT age('week', toDateTime64('2015-01-01 20:30:36.200', 3, 'UTC'), toDateTime64('2015-01-15 20:30:36.200', 3, 'UTC'));
SELECT age('week', toDateTime64('2015-01-01 20:30:36.200', 3, 'UTC'), toDateTime64('2015-01-15 19:30:36.200', 3, 'UTC'));
SELECT age('week', toDateTime64('2015-01-01 20:30:36.200', 3, 'UTC'), toDateTime64('2015-01-15 20:29:36.200', 3, 'UTC'));
SELECT age('week', toDateTime64('2015-01-01 20:30:36.200', 3, 'UTC'), toDateTime64('2015-01-15 20:30:35.200', 3, 'UTC'));
SELECT age('week', toDateTime64('2015-01-01 20:30:36.200', 3, 'UTC'), toDateTime64('2015-01-15 20:30:36.100', 3, 'UTC'));
SELECT age('week', toDateTime64('2015-01-01 20:30:36.200101', 6, 'UTC'), toDateTime64('2015-01-15 20:30:36.200100', 6, 'UTC'));

SELECT age('month', toDateTime64('2015-01-02 20:30:36.200', 3, 'UTC'), toDateTime64('2016-05-02 20:30:36.200', 3, 'UTC'));
SELECT age('month', toDateTime64('2015-01-02 20:30:36.200', 3, 'UTC'), toDateTime64('2016-05-01 20:30:36.200', 3, 'UTC'));
SELECT age('month', toDateTime64('2015-01-02 20:30:36.200', 3, 'UTC'), toDateTime64('2016-05-02 19:30:36.200', 3, 'UTC'));
SELECT age('month', toDateTime64('2015-01-02 20:30:36.200', 3, 'UTC'), toDateTime64('2016-05-02 20:29:36.200', 3, 'UTC'));
SELECT age('month', toDateTime64('2015-01-02 20:30:36.200', 3, 'UTC'), toDateTime64('2016-05-02 20:30:35.200', 3, 'UTC'));
SELECT age('month', toDateTime64('2015-01-02 20:30:36.200', 3, 'UTC'), toDateTime64('2016-05-02 20:30:36.100', 3, 'UTC'));
SELECT age('month', toDateTime64('2015-01-02 20:30:36.200101', 6, 'UTC'), toDateTime64('2016-05-02 20:30:36.200100', 6, 'UTC'));

SELECT age('quarter', toDateTime64('2015-01-02 20:30:36.200', 3, 'UTC'), toDateTime64('2016-04-02 20:30:36.200', 3, 'UTC'));
SELECT age('quarter', toDateTime64('2015-01-02 20:30:36.200', 3, 'UTC'), toDateTime64('2016-04-01 20:30:36.200', 3, 'UTC'));
SELECT age('quarter', toDateTime64('2015-01-02 20:30:36.200', 3, 'UTC'), toDateTime64('2016-04-02 19:30:36.200', 3, 'UTC'));
SELECT age('quarter', toDateTime64('2015-01-02 20:30:36.200', 3, 'UTC'), toDateTime64('2016-04-02 20:29:36.200', 3, 'UTC'));
SELECT age('quarter', toDateTime64('2015-01-02 20:30:36.200', 3, 'UTC'), toDateTime64('2016-04-02 20:30:35.200', 3, 'UTC'));
SELECT age('quarter', toDateTime64('2015-01-02 20:30:36.200', 3, 'UTC'), toDateTime64('2016-04-02 20:30:36.100', 3, 'UTC'));
SELECT age('quarter', toDateTime64('2015-01-02 20:30:36.200101', 6, 'UTC'), toDateTime64('2016-04-02 20:30:36.200100', 6, 'UTC'));

SELECT age('year', toDateTime64('2015-02-02 20:30:36.200', 3, 'UTC'), toDateTime64('2023-02-02 20:30:36.200', 3, 'UTC'));
SELECT age('year', toDateTime64('2015-02-02 20:30:36.200', 3, 'UTC'), toDateTime64('2023-01-02 20:30:36.200', 3, 'UTC'));
SELECT age('year', toDateTime64('2015-02-02 20:30:36.200', 3, 'UTC'), toDateTime64('2023-02-01 20:30:36.200', 3, 'UTC'));
SELECT age('year', toDateTime64('2015-02-02 20:30:36.200', 3, 'UTC'), toDateTime64('2023-02-02 19:30:36.200', 3, 'UTC'));
SELECT age('year', toDateTime64('2015-02-02 20:30:36.200', 3, 'UTC'), toDateTime64('2023-02-02 20:29:36.200', 3, 'UTC'));
SELECT age('year', toDateTime64('2015-02-02 20:30:36.200', 3, 'UTC'), toDateTime64('2023-02-02 20:30:35.200', 3, 'UTC'));
SELECT age('year', toDateTime64('2015-02-02 20:30:36.200', 3, 'UTC'), toDateTime64('2023-02-02 20:30:36.100', 3, 'UTC'));
SELECT age('year', toDateTime64('2015-02-02 20:30:36.200101', 6, 'UTC'), toDateTime64('2023-02-02 20:30:36.200100', 6, 'UTC'));