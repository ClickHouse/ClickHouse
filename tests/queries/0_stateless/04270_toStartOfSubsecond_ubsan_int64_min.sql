-- Regression test for UBSan signed integer overflow at the INT64_MIN boundary
-- in `toStartOfSecond` / `toStartOfMillisecond` / `toStartOfMicrosecond` for
-- `DateTime64`. The bug was surfaced by the `FunctionsStress` property fuzzer
-- on master and 3 unrelated PRs; the body of each function did
-- `datetime64 - fractional_with_sign` in signed `Int64`, which is UB when
-- `datetime64` is near `INT64_MIN`.
SET session_timezone = 'UTC';

SELECT toStartOfSecond(reinterpret(toInt64(-9223372036854775808), 'DateTime64(3)'));
SELECT toStartOfMillisecond(reinterpret(toInt64(-9223372036854775808), 'DateTime64(6)'));
SELECT toStartOfMicrosecond(reinterpret(toInt64(-9223372036854775808), 'DateTime64(9)'));

-- Normal positive inputs still produce the expected result.
SELECT toStartOfSecond(toDateTime64('2026-05-20 10:11:12.456', 3, 'UTC'));
SELECT toStartOfMillisecond(toDateTime64('2026-05-20 10:11:12.456789', 6, 'UTC'));
SELECT toStartOfMicrosecond(toDateTime64('2026-05-20 10:11:12.456789123', 9, 'UTC'));

-- Negative non-extreme inputs still round toward negative infinity.
SELECT toStartOfSecond(toDateTime64(-123.456, 3, 'UTC'));
