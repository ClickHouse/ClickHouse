-- https://github.com/ClickHouse/ClickHouse/issues/119583
-- `age('week', ...)` does not share a code path with `date_diff('week', ...)`: it applies its own
-- week adjustment in src/Functions/dateDiff.cpp, comparing toDayOfWeek of both arguments, on top of
-- the toRelativeWeekNum difference. 02457_datediff_via_unix_epoch only covers date_diff, so `age`
-- needs its own regression across the Unix epoch.
--
-- Before the toRelativeWeekNum floor-division fix, the two pre-epoch weeks that straddle
-- 1969-12-29 collapsed onto the same week number, which made `age` undercount by one week -- and
-- for a one-day forward interval it went negative.

SELECT 'one day across the 1969-12-29 week boundary (was -1)';
SELECT age('week', toDate32('1969-12-28'), toDate32('1969-12-29'));
SELECT age('week', toDateTime64('1969-12-28 00:00:00.000', 3), toDateTime64('1969-12-29 00:00:00.000', 3));

SELECT 'two months across the epoch (was 7)';
SELECT age('week', toDate32('1969-12-01'), toDate32('1970-02-01'));
SELECT age('week', toDateTime64('1969-12-01 00:00:00.000', 3), toDateTime64('1970-02-01 00:00:00.000', 3));

SELECT 'partial weeks either side of the epoch (was 0)';
SELECT age('week', toDate32('1969-12-25'), toDate32('1970-01-05'));
SELECT age('week', toDateTime64('1969-12-25 10:00:00.000', 3), toDateTime64('1970-01-05 10:00:00.000', 3));

-- The week adjustment in src/Functions/dateDiff.cpp compares `toDayOfWeek` of both arguments and,
-- only when those are equal, their time of day. The time-of-day chain used to sit *outside* the
-- `x_day_of_week == y_day_of_week` guard, so `age('week', ...)` also decremented when the end fell
-- on a later weekday than the start but at an earlier time of day.

SELECT 'later weekday, earlier time of day (was 0)';
SELECT age('week', toDateTime64('1969-12-30 01:30:00.000', 3, 'UTC'), toDateTime64('1970-01-07 01:15:00.000', 3, 'UTC'));
SELECT age('week', toDateTime64('1969-12-30 01:15:30.000', 3, 'UTC'), toDateTime64('1970-01-07 01:15:10.000', 3, 'UTC'));

SELECT 'the same, entirely before the epoch (was -1)';
SELECT age('week', toDateTime64('1969-12-23 01:30:00.000', 3, 'UTC'), toDateTime64('1969-12-31 01:15:00.000', 3, 'UTC'));

SELECT 'control: on the same weekday an earlier time of day still rounds down';
SELECT age('week', toDateTime64('1969-12-30 01:30:00.000', 3, 'UTC'), toDateTime64('1970-01-06 01:15:00.000', 3, 'UTC'));

-- The weekday-guard defect is not epoch-related at all: the same shape is wrong far from 1970.
-- This is what proves it is a second, independent bug rather than fallout of the
-- `toRelativeWeekNum` floor-division fix that the rest of this file covers.
SELECT 'later weekday, earlier time of day, nowhere near the epoch (was 0)';
SELECT age('week', toDateTime64('2000-01-04 01:30:00.000', 3, 'UTC'), toDateTime64('2000-01-12 01:15:00.000', 3, 'UTC'));

SELECT 'control: same weekday, nowhere near the epoch';
SELECT age('week', toDateTime64('2000-01-04 01:30:00.000', 3, 'UTC'), toDateTime64('2000-01-11 01:15:00.000', 3, 'UTC'));

-- For `x > y` the adjustment swaps the two arguments into chronological order (`a_comp` / `b_comp`),
-- so the weekdays must be swapped with them. They used to be compared in argument order, which ran
-- the weekday test in the opposite direction from the time-of-day test.
SELECT 'reversed arguments: later weekday is the start (was -103)';
SELECT age('week', toDate('2017-12-31'), toDate('2016-01-01'));

SELECT 'reversed arguments: later weekday, earlier time of day (was 0)';
SELECT age('week', toDateTime64('2000-01-12 01:15:00.000', 3, 'UTC'), toDateTime64('2000-01-04 01:30:00.000', 3, 'UTC'));
SELECT age('week', toDateTime64('1970-01-07 01:15:00.000', 3, 'UTC'), toDateTime64('1969-12-30 01:30:00.000', 3, 'UTC'));

SELECT 'reversed arguments control: same weekday, less than a full week';
SELECT age('week', toDateTime64('2000-01-11 01:15:00.000', 3, 'UTC'), toDateTime64('2000-01-04 01:30:00.000', 3, 'UTC'));
