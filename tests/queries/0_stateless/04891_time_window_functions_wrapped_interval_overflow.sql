-- An interval whose span in seconds is a multiple of 2^32 used to make its subtraction a no-op in
-- the wrapping UInt32 arithmetic of the time window functions, dodging the `wstart > wend` time
-- overflow guard, after which the window-searching loop of `hop` wrapped around zero and never
-- terminated. With constant arguments the loop runs at analysis time, during constant folding,
-- where the query cannot even be killed.
-- https://github.com/ClickHouse/ClickHouse/issues/114605
SELECT hop(toDateTime32('1969-12-31'), toIntervalDay(1), toIntervalDay(2147483648), 'US/Samoa'); -- { serverError BAD_ARGUMENTS }
-- The subtraction of the hop can also leave the time unchanged: a Date bound saturated at the end of the calendar,
-- or a hop that is a multiple of 2^32 seconds when the window starts after the time.
SELECT hopEnd(toDateTime('1970-01-01 01:02:07', 'Europe/Amsterdam'), toIntervalQuarter(413844), toIntervalQuarter(413844)); -- { serverError BAD_ARGUMENTS }
SELECT hop(materialize(toDateTime(0, 'UTC')), toIntervalMonth(100000), toIntervalMonth(100000)); -- { serverError BAD_ARGUMENTS }
SELECT hop(toDateTime(0, 'US/Samoa'), toIntervalDay(33554432), toIntervalDay(52543755)); -- { serverError BAD_ARGUMENTS }
-- If the unchanged end is already before the time, a window that does not contain the time used to be returned.
SELECT hop(toDateTime(1700000000, 'Europe/Amsterdam'), toIntervalDay(33554432), toIntervalDay(52543755)); -- { serverError BAD_ARGUMENTS }
-- A sane hop is unaffected.
SELECT hop(toDateTime('2026-08-13 10:07:00', 'UTC'), toIntervalMinute(15), toIntervalMinute(60), 'UTC');
SELECT hop(toDateTime('2026-08-13 10:07:00', 'UTC'), toIntervalQuarter(1), toIntervalQuarter(2), 'UTC');
