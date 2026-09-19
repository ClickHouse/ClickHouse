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
