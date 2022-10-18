-- { echo }

-- Date32 vs Date32
SELECT dateDiff('second', toDate32('1900-01-01'), toDate32('1900-01-02'));
SELECT dateDiff('minute', toDate32('1900-01-01'), toDate32('1900-01-02'));
SELECT dateDiff('hour', toDate32('1900-01-01'), toDate32('1900-01-02'));
SELECT dateDiff('day', toDate32('1900-01-01'), toDate32('1900-01-02'));
SELECT dateDiff('week', toDate32('1900-01-01'), toDate32('1900-01-08'));
SELECT dateDiff('month', toDate32('1900-01-01'), toDate32('1900-02-01'));
SELECT dateDiff('quarter', toDate32('1900-01-01'), toDate32('1900-04-01'));
SELECT dateDiff('year', toDate32('1900-01-01'), toDate32('1901-01-01'));

-- With DateTime64
-- Date32 vs DateTime64
SELECT dateDiff('second', toDate32('1900-01-01'), toDateTime64('1900-01-02 00:00:00', 3));
SELECT dateDiff('minute', toDate32('1900-01-01'), toDateTime64('1900-01-02 00:00:00', 3));
SELECT dateDiff('hour', toDate32('1900-01-01'), toDateTime64('1900-01-02 00:00:00', 3));
SELECT dateDiff('day', toDate32('1900-01-01'), toDateTime64('1900-01-02 00:00:00', 3));
SELECT dateDiff('week', toDate32('1900-01-01'), toDateTime64('1900-01-08 00:00:00', 3));
SELECT dateDiff('month', toDate32('1900-01-01'), toDateTime64('1900-02-01 00:00:00', 3));
SELECT dateDiff('quarter', toDate32('1900-01-01'), toDateTime64('1900-04-01 00:00:00', 3));
SELECT dateDiff('year', toDate32('1900-01-01'), toDateTime64('1901-01-01 00:00:00', 3));

-- DateTime64 vs Date32
SELECT dateDiff('second', toDateTime64('1900-01-01 00:00:00', 3), toDate32('1900-01-02'));
SELECT dateDiff('minute', toDateTime64('1900-01-01 00:00:00', 3), toDate32('1900-01-02'));
SELECT dateDiff('hour', toDateTime64('1900-01-01 00:00:00', 3), toDate32('1900-01-02'));
SELECT dateDiff('day', toDateTime64('1900-01-01 00:00:00', 3), toDate32('1900-01-02'));
SELECT dateDiff('week', toDateTime64('1900-01-01 00:00:00', 3), toDate32('1900-01-08'));
SELECT dateDiff('month', toDateTime64('1900-01-01 00:00:00', 3), toDate32('1900-02-01'));
SELECT dateDiff('quarter', toDateTime64('1900-01-01 00:00:00', 3), toDate32('1900-04-01'));
SELECT dateDiff('year', toDateTime64('1900-01-01 00:00:00', 3), toDate32('1901-01-01'));

-- With DateTime
-- Date32 vs DateTime
SELECT dateDiff('second', toDate32('2015-08-18'), toDateTime('2015-08-19 00:00:00'));
SELECT dateDiff('minute', toDate32('2015-08-18'), toDateTime('2015-08-19 00:00:00'));
SELECT dateDiff('hour', toDate32('2015-08-18'), toDateTime('2015-08-19 00:00:00'));
SELECT dateDiff('day', toDate32('2015-08-18'), toDateTime('2015-08-19 00:00:00'));
SELECT dateDiff('week', toDate32('2015-08-18'), toDateTime('2015-08-25 00:00:00'));
SELECT dateDiff('month', toDate32('2015-08-18'), toDateTime('2015-09-18 00:00:00'));
SELECT dateDiff('quarter', toDate32('2015-08-18'), toDateTime('2015-11-18 00:00:00'));
SELECT dateDiff('year', toDate32('2015-08-18'), toDateTime('2016-08-18 00:00:00'));

-- DateTime vs Date32
SELECT dateDiff('second', toDateTime('2015-08-18 00:00:00'), toDate32('2015-08-19'));
SELECT dateDiff('minute', toDateTime('2015-08-18 00:00:00'), toDate32('2015-08-19'));
SELECT dateDiff('hour', toDateTime('2015-08-18 00:00:00'), toDate32('2015-08-19'));
SELECT dateDiff('day', toDateTime('2015-08-18 00:00:00'), toDate32('2015-08-19'));
SELECT dateDiff('week', toDateTime('2015-08-18 00:00:00'), toDate32('2015-08-25'));
SELECT dateDiff('month', toDateTime('2015-08-18 00:00:00'), toDate32('2015-09-18'));
SELECT dateDiff('quarter', toDateTime('2015-08-18 00:00:00'), toDate32('2015-11-18'));
SELECT dateDiff('year', toDateTime('2015-08-18 00:00:00'), toDate32('2016-08-18'));

-- With Date
-- Date32 vs Date
SELECT dateDiff('second', toDate32('2015-08-18'), toDate('2015-08-19'));
SELECT dateDiff('minute', toDate32('2015-08-18'), toDate('2015-08-19'));
SELECT dateDiff('hour', toDate32('2015-08-18'), toDate('2015-08-19'));
SELECT dateDiff('day', toDate32('2015-08-18'), toDate('2015-08-19'));
SELECT dateDiff('week', toDate32('2015-08-18'), toDate('2015-08-25'));
SELECT dateDiff('month', toDate32('2015-08-18'), toDate('2015-09-18'));
SELECT dateDiff('quarter', toDate32('2015-08-18'), toDate('2015-11-18'));
SELECT dateDiff('year', toDate32('2015-08-18'), toDate('2016-08-18'));

-- Date vs Date32
SELECT dateDiff('second', toDate('2015-08-18'), toDate32('2015-08-19'));
SELECT dateDiff('minute', toDate('2015-08-18'), toDate32('2015-08-19'));
SELECT dateDiff('hour', toDate('2015-08-18'), toDate32('2015-08-19'));
SELECT dateDiff('day', toDate('2015-08-18'), toDate32('2015-08-19'));
SELECT dateDiff('week', toDate('2015-08-18'), toDate32('2015-08-25'));
SELECT dateDiff('month', toDate('2015-08-18'), toDate32('2015-09-18'));
SELECT dateDiff('quarter', toDate('2015-08-18'), toDate32('2015-11-18'));
SELECT dateDiff('year', toDate('2015-08-18'), toDate32('2016-08-18'));

-- Const vs non-const columns
SELECT dateDiff('day', toDate32('1900-01-01'), materialize(toDate32('1900-01-02')));
SELECT dateDiff('day', toDate32('1900-01-01'), materialize(toDateTime64('1900-01-02 00:00:00', 3)));
SELECT dateDiff('day', toDateTime64('1900-01-01 00:00:00', 3), materialize(toDate32('1900-01-02')));
SELECT dateDiff('day', toDate32('2015-08-18'), materialize(toDateTime('2015-08-19 00:00:00')));
SELECT dateDiff('day', toDateTime('2015-08-18 00:00:00'), materialize(toDate32('2015-08-19')));
SELECT dateDiff('day', toDate32('2015-08-18'), materialize(toDate('2015-08-19')));
SELECT dateDiff('day', toDate('2015-08-18'), materialize(toDate32('2015-08-19')));

-- Non-const vs const columns
SELECT dateDiff('day', materialize(toDate32('1900-01-01')), toDate32('1900-01-02'));
SELECT dateDiff('day', materialize(toDate32('1900-01-01')), toDateTime64('1900-01-02 00:00:00', 3));
SELECT dateDiff('day', materialize(toDateTime64('1900-01-01 00:00:00', 3)), toDate32('1900-01-02'));
SELECT dateDiff('day', materialize(toDate32('2015-08-18')), toDateTime('2015-08-19 00:00:00'));
SELECT dateDiff('day', materialize(toDateTime('2015-08-18 00:00:00')), toDate32('2015-08-19'));
SELECT dateDiff('day', materialize(toDate32('2015-08-18')), toDate('2015-08-19'));
SELECT dateDiff('day', materialize(toDate('2015-08-18')), toDate32('2015-08-19'));

-- Non-const vs non-const columns
SELECT dateDiff('day', materialize(toDate32('1900-01-01')), materialize(toDate32('1900-01-02')));
SELECT dateDiff('day', materialize(toDate32('1900-01-01')), materialize(toDateTime64('1900-01-02 00:00:00', 3)));
SELECT dateDiff('day', materialize(toDateTime64('1900-01-01 00:00:00', 3)), materialize(toDate32('1900-01-02')));
SELECT dateDiff('day', materialize(toDate32('2015-08-18')), materialize(toDateTime('2015-08-19 00:00:00')));
SELECT dateDiff('day', materialize(toDateTime('2015-08-18 00:00:00')), materialize(toDate32('2015-08-19')));
SELECT dateDiff('day', materialize(toDate32('2015-08-18')), materialize(toDate('2015-08-19')));
SELECT dateDiff('day', materialize(toDate('2015-08-18')), materialize(toDate32('2015-08-19')));
