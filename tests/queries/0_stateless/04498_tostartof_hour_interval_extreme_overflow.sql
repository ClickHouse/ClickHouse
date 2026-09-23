-- Regression test for a signed integer overflow (reported by UndefinedBehaviorSanitizer) in
-- `DateLUTImpl::toStartOfHourInterval` when rounding an extreme `DateTime64` value down to the
-- start of a multi-hour interval. The result is reconstructed as `date + time`, and that addition
-- overflowed past the boundary of `Int64` for values far outside any valid date range.
--
-- This complements 04415_tostartof_datetime64_extreme_overflow: there the `INTERVAL 1 HOUR` case is
-- short-circuited to `toStartOfHour` and never reaches the overflowing path, so only intervals of
-- more than one hour exercise it.

-- Normal values must still be rounded down to the start of the multi-hour interval correctly.
SELECT toStartOfInterval(toDateTime('2021-06-22 03:17:42', 'UTC'), INTERVAL 2 HOUR);
SELECT toStartOfInterval(toDateTime('2021-06-22 03:17:42', 'UTC'), INTERVAL 3 HOUR);
SELECT toStartOfInterval(toDateTime('2021-06-22 03:17:42', 'UTC'), INTERVAL 5 HOUR);
SELECT toStartOfInterval(toDateTime('2021-06-22 13:17:42', 'UTC'), INTERVAL 5 HOUR);

-- Extreme `DateTime64` values with multi-hour intervals must not trigger undefined behaviour. These
-- are constant expressions, so they are evaluated during query analysis (where the overflow was found).
-- The result for such out-of-range values is implementation-defined, so it is discarded; the test only
-- requires that the evaluation does not overflow.
SELECT toStartOfInterval(toDateTime64(-9223372036854775807, 0, 'UTC'), INTERVAL 2 HOUR) FORMAT Null;
SELECT toStartOfInterval(toDateTime64(-9223372036854775807, 0, 'UTC'), INTERVAL 5 HOUR) FORMAT Null;
SELECT toStartOfInterval(toDateTime64(-9223372036854775807, 0, 'UTC'), INTERVAL 257 HOUR) FORMAT Null;
SELECT toStartOfInterval(toDateTime64(9223372036854775807, 0, 'UTC'), INTERVAL 257 HOUR) FORMAT Null;

-- Extreme interval counts must also be safe. The hour count is only validated to be positive, so
-- `hours * 3600` can wrap. `INTERVAL 4611686018427387904 HOUR` wraps the seconds divisor to exactly
-- zero, which used to divide by zero before the saturating guard. The divisor is saturated to the
-- maximum instead, so a normal value rounds down to the start of its day.
SELECT toStartOfInterval(toDateTime('2021-06-22 03:17:42', 'UTC'), INTERVAL 4611686018427387904 HOUR);
SELECT toStartOfInterval(toDateTime64(-9223372036854775807, 0, 'UTC'), INTERVAL 4611686018427387904 HOUR) FORMAT Null;
SELECT toStartOfInterval(toDateTime64(9223372036854775807, 0, 'UTC'), INTERVAL 9223372036854775807 HOUR) FORMAT Null;

SELECT 'ok';
