-- { echo }

-- Date32 vs Date32
SELECT dateDiff('second', toDate32('1927-01-01', 'UTC'), toDate32('1927-01-02', 'UTC'), 'UTC');
SELECT dateDiff('minute', toDate32('1927-01-01', 'UTC'), toDate32('1927-01-02', 'UTC'), 'UTC');
SELECT dateDiff('hour', toDate32('1927-01-01', 'UTC'), toDate32('1927-01-02', 'UTC'), 'UTC');
SELECT dateDiff('day', toDate32('1927-01-01', 'UTC'), toDate32('1927-01-02', 'UTC'), 'UTC');
SELECT dateDiff('week', toDate32('1927-01-01', 'UTC'), toDate32('1927-01-08', 'UTC'), 'UTC');
SELECT dateDiff('month', toDate32('1927-01-01', 'UTC'), toDate32('1927-02-01', 'UTC'), 'UTC');
SELECT dateDiff('quarter', toDate32('1927-01-01', 'UTC'), toDate32('1927-04-01', 'UTC'), 'UTC');
SELECT dateDiff('year', toDate32('1927-01-01', 'UTC'), toDate32('1928-01-01', 'UTC'), 'UTC');

-- With DateTime64
-- Date32 vs DateTime64
SELECT dateDiff('second', toDate32('1927-01-01', 'UTC'), toDateTime64('1927-01-02 00:00:00', 3, 'UTC'), 'UTC');
SELECT dateDiff('minute', toDate32('1927-01-01', 'UTC'), toDateTime64('1927-01-02 00:00:00', 3, 'UTC'), 'UTC');
SELECT dateDiff('hour', toDate32('1927-01-01', 'UTC'), toDateTime64('1927-01-02 00:00:00', 3, 'UTC'), 'UTC');
SELECT dateDiff('day', toDate32('1927-01-01', 'UTC'), toDateTime64('1927-01-02 00:00:00', 3, 'UTC'), 'UTC');
SELECT dateDiff('week', toDate32('1927-01-01', 'UTC'), toDateTime64('1927-01-08 00:00:00', 3, 'UTC'), 'UTC');
SELECT dateDiff('month', toDate32('1927-01-01', 'UTC'), toDateTime64('1927-02-01 00:00:00', 3, 'UTC'), 'UTC');
SELECT dateDiff('quarter', toDate32('1927-01-01', 'UTC'), toDateTime64('1927-04-01 00:00:00', 3, 'UTC'), 'UTC');
SELECT dateDiff('year', toDate32('1927-01-01', 'UTC'), toDateTime64('1928-01-01 00:00:00', 3, 'UTC'), 'UTC');

-- DateTime64 vs Date32
SELECT dateDiff('second', toDateTime64('1927-01-01 00:00:00', 3, 'UTC'), toDate32('1927-01-02', 'UTC'), 'UTC');
SELECT dateDiff('minute', toDateTime64('1927-01-01 00:00:00', 3, 'UTC'), toDate32('1927-01-02', 'UTC'), 'UTC');
SELECT dateDiff('hour', toDateTime64('1927-01-01 00:00:00', 3, 'UTC'), toDate32('1927-01-02', 'UTC'), 'UTC');
SELECT dateDiff('day', toDateTime64('1927-01-01 00:00:00', 3, 'UTC'), toDate32('1927-01-02', 'UTC'), 'UTC');
SELECT dateDiff('week', toDateTime64('1927-01-01 00:00:00', 3, 'UTC'), toDate32('1927-01-08', 'UTC'), 'UTC');
SELECT dateDiff('month', toDateTime64('1927-01-01 00:00:00', 3, 'UTC'), toDate32('1927-02-01', 'UTC'), 'UTC');
SELECT dateDiff('quarter', toDateTime64('1927-01-01 00:00:00', 3, 'UTC'), toDate32('1927-04-01', 'UTC'), 'UTC');
SELECT dateDiff('year', toDateTime64('1927-01-01 00:00:00', 3, 'UTC'), toDate32('1928-01-01', 'UTC'), 'UTC');

-- With DateTime
-- Date32 vs DateTime
SELECT dateDiff('second', toDate32('2015-08-18', 'UTC'), toDateTime('2015-08-19 00:00:00', 'UTC'), 'UTC');
SELECT dateDiff('minute', toDate32('2015-08-18', 'UTC'), toDateTime('2015-08-19 00:00:00', 'UTC'), 'UTC');
SELECT dateDiff('hour', toDate32('2015-08-18', 'UTC'), toDateTime('2015-08-19 00:00:00', 'UTC'), 'UTC');
SELECT dateDiff('day', toDate32('2015-08-18', 'UTC'), toDateTime('2015-08-19 00:00:00', 'UTC'), 'UTC');
SELECT dateDiff('week', toDate32('2015-08-18', 'UTC'), toDateTime('2015-08-25 00:00:00', 'UTC'), 'UTC');
SELECT dateDiff('month', toDate32('2015-08-18', 'UTC'), toDateTime('2015-09-18 00:00:00', 'UTC'), 'UTC');
SELECT dateDiff('quarter', toDate32('2015-08-18', 'UTC'), toDateTime('2015-11-18 00:00:00', 'UTC'), 'UTC');
SELECT dateDiff('year', toDate32('2015-08-18', 'UTC'), toDateTime('2016-08-18 00:00:00', 'UTC'), 'UTC');

-- DateTime vs Date32
SELECT dateDiff('second', toDateTime('2015-08-18 00:00:00', 'UTC'), toDate32('2015-08-19', 'UTC'), 'UTC');
SELECT dateDiff('minute', toDateTime('2015-08-18 00:00:00', 'UTC'), toDate32('2015-08-19', 'UTC'), 'UTC');
SELECT dateDiff('hour', toDateTime('2015-08-18 00:00:00', 'UTC'), toDate32('2015-08-19', 'UTC'), 'UTC');
SELECT dateDiff('day', toDateTime('2015-08-18 00:00:00', 'UTC'), toDate32('2015-08-19', 'UTC'), 'UTC');
SELECT dateDiff('week', toDateTime('2015-08-18 00:00:00', 'UTC'), toDate32('2015-08-25', 'UTC'), 'UTC');
SELECT dateDiff('month', toDateTime('2015-08-18 00:00:00', 'UTC'), toDate32('2015-09-18', 'UTC'), 'UTC');
SELECT dateDiff('quarter', toDateTime('2015-08-18 00:00:00', 'UTC'), toDate32('2015-11-18', 'UTC'), 'UTC');
SELECT dateDiff('year', toDateTime('2015-08-18 00:00:00', 'UTC'), toDate32('2016-08-18', 'UTC'), 'UTC');

-- With Date
-- Date32 vs Date
SELECT dateDiff('second', toDate32('2015-08-18', 'UTC'), toDate('2015-08-19', 'UTC'), 'UTC');
SELECT dateDiff('minute', toDate32('2015-08-18', 'UTC'), toDate('2015-08-19', 'UTC'), 'UTC');
SELECT dateDiff('hour', toDate32('2015-08-18', 'UTC'), toDate('2015-08-19', 'UTC'), 'UTC');
SELECT dateDiff('day', toDate32('2015-08-18', 'UTC'), toDate('2015-08-19', 'UTC'), 'UTC');
SELECT dateDiff('week', toDate32('2015-08-18', 'UTC'), toDate('2015-08-25', 'UTC'), 'UTC');
SELECT dateDiff('month', toDate32('2015-08-18', 'UTC'), toDate('2015-09-18', 'UTC'), 'UTC');
SELECT dateDiff('quarter', toDate32('2015-08-18', 'UTC'), toDate('2015-11-18', 'UTC'), 'UTC');
SELECT dateDiff('year', toDate32('2015-08-18', 'UTC'), toDate('2016-08-18', 'UTC'), 'UTC');

-- Date vs Date32
SELECT dateDiff('second', toDate('2015-08-18', 'UTC'), toDate32('2015-08-19', 'UTC'), 'UTC');
SELECT dateDiff('minute', toDate('2015-08-18', 'UTC'), toDate32('2015-08-19', 'UTC'), 'UTC');
SELECT dateDiff('hour', toDate('2015-08-18', 'UTC'), toDate32('2015-08-19', 'UTC'), 'UTC');
SELECT dateDiff('day', toDate('2015-08-18', 'UTC'), toDate32('2015-08-19', 'UTC'), 'UTC');
SELECT dateDiff('week', toDate('2015-08-18', 'UTC'), toDate32('2015-08-25', 'UTC'), 'UTC');
SELECT dateDiff('month', toDate('2015-08-18', 'UTC'), toDate32('2015-09-18', 'UTC'), 'UTC');
SELECT dateDiff('quarter', toDate('2015-08-18', 'UTC'), toDate32('2015-11-18', 'UTC'), 'UTC');
SELECT dateDiff('year', toDate('2015-08-18', 'UTC'), toDate32('2016-08-18', 'UTC'), 'UTC');

-- Const vs non-const columns
SELECT dateDiff('day', toDate32('1927-01-01', 'UTC'), materialize(toDate32('1927-01-02', 'UTC')), 'UTC');
SELECT dateDiff('day', toDate32('1927-01-01', 'UTC'), materialize(toDateTime64('1927-01-02 00:00:00', 3, 'UTC')), 'UTC');
SELECT dateDiff('day', toDateTime64('1927-01-01 00:00:00', 3, 'UTC'), materialize(toDate32('1927-01-02', 'UTC')), 'UTC');
SELECT dateDiff('day', toDate32('2015-08-18', 'UTC'), materialize(toDateTime('2015-08-19 00:00:00', 'UTC')), 'UTC');
SELECT dateDiff('day', toDateTime('2015-08-18 00:00:00', 'UTC'), materialize(toDate32('2015-08-19', 'UTC')), 'UTC');
SELECT dateDiff('day', toDate32('2015-08-18', 'UTC'), materialize(toDate('2015-08-19', 'UTC')), 'UTC');
SELECT dateDiff('day', toDate('2015-08-18', 'UTC'), materialize(toDate32('2015-08-19', 'UTC')), 'UTC');

-- Non-const vs const columns
SELECT dateDiff('day', materialize(toDate32('1927-01-01', 'UTC')), toDate32('1927-01-02', 'UTC'), 'UTC');
SELECT dateDiff('day', materialize(toDate32('1927-01-01', 'UTC')), toDateTime64('1927-01-02 00:00:00', 3, 'UTC'), 'UTC');
SELECT dateDiff('day', materialize(toDateTime64('1927-01-01 00:00:00', 3, 'UTC')), toDate32('1927-01-02', 'UTC'), 'UTC');
SELECT dateDiff('day', materialize(toDate32('2015-08-18', 'UTC')), toDateTime('2015-08-19 00:00:00', 'UTC'), 'UTC');
SELECT dateDiff('day', materialize(toDateTime('2015-08-18 00:00:00', 'UTC')), toDate32('2015-08-19', 'UTC'), 'UTC');
SELECT dateDiff('day', materialize(toDate32('2015-08-18', 'UTC')), toDate('2015-08-19', 'UTC'), 'UTC');
SELECT dateDiff('day', materialize(toDate('2015-08-18', 'UTC')), toDate32('2015-08-19', 'UTC'), 'UTC');

-- Non-const vs non-const columns
SELECT dateDiff('day', materialize(toDate32('1927-01-01', 'UTC')), materialize(toDate32('1927-01-02', 'UTC')), 'UTC');
SELECT dateDiff('day', materialize(toDate32('1927-01-01', 'UTC')), materialize(toDateTime64('1927-01-02 00:00:00', 3, 'UTC')), 'UTC');
SELECT dateDiff('day', materialize(toDateTime64('1927-01-01 00:00:00', 3, 'UTC')), materialize(toDate32('1927-01-02', 'UTC')), 'UTC');
SELECT dateDiff('day', materialize(toDate32('2015-08-18', 'UTC')), materialize(toDateTime('2015-08-19 00:00:00', 'UTC')), 'UTC');
SELECT dateDiff('day', materialize(toDateTime('2015-08-18 00:00:00', 'UTC')), materialize(toDate32('2015-08-19', 'UTC')), 'UTC');
SELECT dateDiff('day', materialize(toDate32('2015-08-18', 'UTC')), materialize(toDate('2015-08-19', 'UTC')), 'UTC');
SELECT dateDiff('day', materialize(toDate('2015-08-18', 'UTC')), materialize(toDate32('2015-08-19', 'UTC')), 'UTC');
