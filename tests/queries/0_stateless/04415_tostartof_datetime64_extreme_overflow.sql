-- Regression test for a signed integer overflow (reported by UndefinedBehaviorSanitizer)
-- in `DateLUTImpl::toStartOfMinuteInterval` and `DateLUTImpl::roundDown` when rounding an
-- extreme `DateTime64` value down to the start of an interval. The expression
-- `t + 1 - divisor` overflowed below the minimum of `Int64` for values close to its minimum.

-- Normal values must still be rounded down correctly.
SELECT toStartOfMinute(toDateTime('2021-06-22 03:17:42', 'UTC'));
SELECT toStartOfFiveMinutes(toDateTime('2021-06-22 03:17:42', 'UTC'));
SELECT toStartOfTenMinutes(toDateTime('2021-06-22 03:17:42', 'UTC'));
SELECT toStartOfFifteenMinutes(toDateTime('2021-06-22 03:17:42', 'UTC'));
SELECT toStartOfHour(toDateTime('2021-06-22 03:17:42', 'UTC'));
SELECT toStartOfInterval(toDateTime('2021-06-22 03:17:42', 'UTC'), INTERVAL 5 MINUTE);

-- Extreme `DateTime64` values must not trigger undefined behaviour. These are constant
-- expressions, so they are evaluated during query analysis (where the overflow was found).
-- The result for such out-of-range values is implementation-defined, so it is discarded;
-- the test only requires that the evaluation does not overflow.
SELECT toStartOfMinute(toDateTime64(-9223372036854775807, 0, 'UTC')) FORMAT Null;
SELECT toStartOfFiveMinutes(toDateTime64(-9223372036854775807, 0, 'UTC')) FORMAT Null;
SELECT toStartOfTenMinutes(toDateTime64(-9223372036854775807, 0, 'UTC')) FORMAT Null;
SELECT toStartOfFifteenMinutes(toDateTime64(-9223372036854775807, 0, 'UTC')) FORMAT Null;
SELECT toStartOfHour(toDateTime64(-9223372036854775807, 0, 'UTC')) FORMAT Null;
SELECT toStartOfInterval(toDateTime64(-9223372036854775807, 0, 'UTC'), INTERVAL 5 MINUTE) FORMAT Null;
SELECT toStartOfInterval(toDateTime64(-9223372036854775807, 0, 'UTC'), INTERVAL 1 HOUR) FORMAT Null;
SELECT toStartOfInterval(toDateTime64(-9223372036854775807, 0, 'UTC'), INTERVAL 90 SECOND) FORMAT Null;

SELECT 'ok';
