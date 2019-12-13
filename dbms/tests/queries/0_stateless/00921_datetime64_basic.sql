USE test;

DROP TABLE IF EXISTS A;

SELECT CAST(1 as DateTime64('abc')); -- { serverError 43 } # Invalid scale parameter type
SELECT CAST(1 as DateTime64(100)); -- { serverError 69 } # too big scale
SELECT CAST(1 as DateTime64(-1)); -- { serverError 43 } # signed scale parameter type
SELECT CAST(1 as DateTime64(3, 'qqq')); -- { serverError 1000 } # invalid timezone
SELECT toDateTime64('2019-09-16 19:20:11.234', 3, 'qqq'); -- { serverError 1000 } # invalid timezone
SELECT toDateTime64('2019-09-16 19:20:11', 3, 'UTC'); -- this now works OK and produces timestamp with no subsecond part

CREATE TABLE A(t DateTime64(3, 'UTC')) ENGINE = MergeTree() ORDER BY t;
INSERT INTO A(t) VALUES ('2019-05-03 11:25:25.123456789');

SELECT toString(t, 'UTC'), toDate(t), toStartOfDay(t), toStartOfQuarter(t), toTime(t), toStartOfMinute(t) FROM A ORDER BY t;

SELECT toDateTime64('2019-09-16 19:20:11.234', 3, 'Europe/Minsk');

DROP TABLE A;