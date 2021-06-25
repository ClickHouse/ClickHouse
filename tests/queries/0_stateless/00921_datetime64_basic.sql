DROP TABLE IF EXISTS A;

SELECT CAST(1 as DateTime64('abc')); -- { serverError 43 } # Invalid scale parameter type
SELECT CAST(1 as DateTime64(100)); -- { serverError 69 } # too big scale
SELECT CAST(1 as DateTime64(-1)); -- { serverError 43 } # signed scale parameter type
SELECT CAST(1 as DateTime64(3, 'qqq')); -- { serverError 1000 } # invalid timezone

SELECT toDateTime64('2019-09-16 19:20:11.234', 'abc'); -- { serverError 43 } # invalid scale
SELECT toDateTime64('2019-09-16 19:20:11.234', 100); -- { serverError 69 } # too big scale
SELECT toDateTime64(CAST([['CLb5Ph ']], 'String'), uniqHLL12('2Gs1V', 752)); -- { serverError 44 } # non-const string and non-const scale
SELECT toDateTime64('2019-09-16 19:20:11.234', 3, 'qqq'); -- { serverError 1000 } # invalid timezone

SELECT ignore(now64(gccMurmurHash())); -- { serverError 43 } # Illegal argument type
SELECT ignore(now64('abcd')); -- { serverError 43 } # Illegal argument type
SELECT ignore(now64(number)) FROM system.numbers LIMIT 10; -- { serverError 43 } # Illegal argument type

SELECT toDateTime64('2019-09-16 19:20:11', 3, 'UTC'); -- this now works OK and produces timestamp with no subsecond part

CREATE TABLE A(t DateTime64(3, 'UTC')) ENGINE = MergeTree() ORDER BY t;
INSERT INTO A(t) VALUES ('2019-05-03 11:25:25.123456789');

SELECT toString(t, 'UTC'), toDate(t), toStartOfDay(t), toStartOfQuarter(t), toTime(t), toStartOfMinute(t) FROM A ORDER BY t;

SELECT toDateTime64('2019-09-16 19:20:11.234', 3, 'Europe/Minsk');

DROP TABLE A;
