USE test;

DROP TABLE IF EXISTS A;

SELECT CAST(1 as DateTime64('abc')); -- { serverError 43 } # Invalid scale parameter type
SELECT CAST(1 as DateTime64(100)); -- { serverError 69 } # too big scale
SELECT CAST(1 as DateTime64(-1)); -- { serverError 43 } # signed scale parameter type
SELECT CAST(1 as DateTime64(3, 'qqq')); -- { serverError 1000 } # invalid timezone
SELECT toDateTime64('2019-09-16 19:20:11.234', 3, 'qqq'); -- { serverError 1000 } # invalid timezone
SELECT toDateTime64('2019-09-16 19:20', 3, 'qqq'); -- { serverError 1000 } # invalid timezone

SELECT toDateTime64('2019-09-16 19:20:11', 3), ignore(now64(3));

CREATE TABLE A(t DateTime64(3, 'UTC')) ENGINE = MergeTree() ORDER BY t;
INSERT INTO A(t) VALUES (1556879125123456789), ('2019-05-03 11:25:25.123456789');

SELECT toString(t, 'UTC'), toDate(t), toStartOfDay(t), toStartOfQuarter(t), toTime(t), toStartOfMinute(t) FROM A ORDER BY t;

SELECT toDateTime64('2019-09-16 19:20:11.234', 3, 'Europe/Minsk'), toDateTime64('2019-09-16 19:20:11.234', 3), toDateTime64(1234567891011, 3);



DROP TABLE A;
 -- issue toDate does a reinterpret_cast of the datetime64 which is incorrect
-- for the example above, it returns 2036-08-23 which is 0x5F15 days after epoch
-- the datetime64 is 0x159B2550CB345F15