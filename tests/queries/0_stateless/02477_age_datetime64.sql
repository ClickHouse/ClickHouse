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

-- UBsan bug #66638
set session_timezone = 'UTC';
SELECT age('second', toDateTime(1157339245694594829, 6, 'UTC'), toDate('2015-08-18'))
