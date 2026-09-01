-- An interval whose span in seconds is a multiple of 2^32 used to make its subtraction a no-op in
-- the wrapping UInt32 arithmetic of the time window functions, dodging the `wstart > wend` time
-- overflow guard, after which the window-searching loop of `hop` wrapped around zero and never
-- terminated. With constant arguments the loop runs at analysis time, during constant folding,
-- where the query cannot even be killed.
-- https://github.com/ClickHouse/ClickHouse/issues/114605
SELECT hop(toDateTime32('1969-12-31'), toIntervalDay(1), toIntervalDay(2147483648), 'US/Samoa'); -- { serverError BAD_ARGUMENTS }
-- A sane hop is unaffected.
SELECT hop(toDateTime('2026-08-13 10:07:00', 'UTC'), toIntervalMinute(15), toIntervalMinute(60), 'UTC');
