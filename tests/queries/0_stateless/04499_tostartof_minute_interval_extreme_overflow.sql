-- Regression test for a division by zero in `DateLUTImpl::toStartOfMinuteInterval` with an extreme
-- interval count. This is the `MINUTE` sibling of the `toStartOfHourInterval` overflow covered by
-- 04498_tostartof_hour_interval_extreme_overflow: `toStartOfInterval` only validates that the interval
-- value is positive, so a huge but positive `toIntervalMinute` reaches `toStartOfMinuteInterval`, where
-- `60 * minutes` wraps. `INTERVAL 4611686018427387904 MINUTE` wraps the divisor to exactly zero
-- (`60 * 2^62 ≡ 0 mod 2^64`), and the following division by it is undefined behaviour (a division-by-zero
-- trap on x86 and a sanitizer report under UndefinedBehaviorSanitizer). The divisor is now saturated to
-- the maximum instead, so such a meaningless interval count rounds down to the start of the epoch.

-- Normal values must still be rounded down to the start of the multi-minute interval correctly
-- (the rounding is anchored at the epoch, not at midnight).
SELECT toStartOfInterval(toDateTime('2021-06-22 03:17:42', 'UTC'), INTERVAL 5 MINUTE);
SELECT toStartOfInterval(toDateTime('2021-06-22 03:17:42', 'UTC'), INTERVAL 15 MINUTE);
SELECT toStartOfInterval(toDateTime('2021-06-22 03:17:42', 'UTC'), INTERVAL 30 MINUTE);
SELECT toStartOfInterval(toDateTime('2021-06-22 03:17:42', 'UTC'), INTERVAL 17 MINUTE);
SELECT toStartOfInterval(toDateTime('2021-06-22 13:42:42', 'UTC'), INTERVAL 7 MINUTE);

-- Extreme interval count: `60 * minutes` wraps to exactly zero, which used to divide by zero before the
-- saturating guard. The divisor is saturated to the maximum instead, so a normal value rounds down to
-- the start of the epoch. These are constant expressions, evaluated during query analysis (where the
-- overflow was found).
SELECT toStartOfInterval(toDateTime('2021-06-22 03:17:42', 'UTC'), INTERVAL 4611686018427387904 MINUTE);

-- Extreme `DateTime64` values combined with extreme interval counts must also be safe. The result for
-- such out-of-range values is implementation-defined, so it is discarded; the test only requires that
-- the evaluation does not divide by zero or overflow.
SELECT toStartOfInterval(toDateTime64(-9223372036854775807, 0, 'UTC'), INTERVAL 257 MINUTE) FORMAT Null;
SELECT toStartOfInterval(toDateTime64(-9223372036854775807, 0, 'UTC'), INTERVAL 4611686018427387904 MINUTE) FORMAT Null;
SELECT toStartOfInterval(toDateTime64(9223372036854775807, 0, 'UTC'), INTERVAL 4611686018427387904 MINUTE) FORMAT Null;
SELECT toStartOfInterval(toDateTime64(9223372036854775807, 0, 'UTC'), INTERVAL 9223372036854775807 MINUTE) FORMAT Null;

SELECT 'ok';
