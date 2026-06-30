-- Regression test for a signed integer overflow (reported by UndefinedBehaviorSanitizer) in
-- `DateLUTImpl::roundDownToMultiple` reached from `DateLUTImpl::toStartOfMinuteInterval`. The minute
-- interval divisor is computed as `60 * minutes`; for an extreme interval count it wraps to a negative
-- value (overflowing the boundary check `INT64_MIN + divisor`) or to exactly zero (dividing by zero).
-- Surfaced by the AST fuzzer. Complements 04415_tostartof_datetime64_extreme_overflow, which exercises
-- an extreme value with a small interval count rather than an extreme interval count.
--
-- The extreme `DateTime64` is built with `reinterpret` rather than `toDateTime64(...)`: the latter
-- clamps out-of-range arguments, so it never reaches the overflowing code path. The result for such
-- meaningless inputs is implementation-defined, so it is discarded (`FORMAT Null`); the test only
-- requires that the evaluation does not invoke undefined behaviour.
SET session_timezone = 'UTC';

-- `60 * minutes` wraps to a negative divisor (the reported overflow at DateLUTImpl.h:301).
SELECT toStartOfInterval(reinterpret(toInt64(-9223372036854775808), 'DateTime64(0)'), INTERVAL 4611686018427387902 MINUTE) FORMAT Null;
SELECT toStartOfInterval(reinterpret(toInt64(-9223372036854775808), 'DateTime64(0)'), INTERVAL 9223372036854775807 MINUTE) FORMAT Null;

-- `60 * minutes` wraps to exactly zero (INTERVAL 4611686018427387904 MINUTE == 2^62), which used to
-- divide by zero in both the non-negative and the negative branch.
SELECT toStartOfInterval(toDateTime('2021-06-22 03:17:42', 'UTC'), INTERVAL 4611686018427387904 MINUTE) FORMAT Null;
SELECT toStartOfInterval(reinterpret(toInt64(-9223372036854775808), 'DateTime64(0)'), INTERVAL 4611686018427387904 MINUTE) FORMAT Null;

-- Normal values must still be rounded down to the start of the minute interval correctly.
SELECT toStartOfMinute(toDateTime('2021-06-22 03:17:42', 'UTC'));
SELECT toStartOfFiveMinutes(toDateTime('2021-06-22 03:17:42', 'UTC'));
SELECT toStartOfTenMinutes(toDateTime('2021-06-22 03:17:42', 'UTC'));
SELECT toStartOfFifteenMinutes(toDateTime('2021-06-22 03:17:42', 'UTC'));
SELECT toStartOfInterval(toDateTime('2021-06-22 03:17:42', 'UTC'), INTERVAL 5 MINUTE);

SELECT 'ok';
