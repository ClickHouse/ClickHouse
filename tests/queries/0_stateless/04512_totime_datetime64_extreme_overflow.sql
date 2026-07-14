-- Regression test: converting an extreme DateTime64 (INT64_MIN / INT64_MAX) to Time / Time64
-- in a timezone with a non-zero offset must not trigger UBSan signed-integer overflow at
-- FunctionsConversion.h ((dt64 + offset) % 86400). The result is the local seconds-of-day.
-- Fixed-offset Etc/GMT zones are used on purpose: they have no historical transitions, so
-- timezoneOffset() returns a constant offset even for out-of-range extreme values. Historical
-- zones (e.g. America/New_York) return a tzdata-version-dependent LMT-era offset for such
-- inputs, which makes the exact wall-clock result non-portable.

SET enable_time_time64_type = 1;

-- DateTime64 -> Time (ToTimeImpl::execute(Int64)): INT64_MIN with a negative offset overflowed.
SELECT toTime(reinterpret(CAST(-9223372036854775808 AS Int64), 'DateTime64(0, ''Etc/GMT+12'')'));
-- INT64_MAX with a positive offset overflowed in the other direction.
SELECT toTime(reinterpret(CAST(9223372036854775807 AS Int64), 'DateTime64(0, ''Etc/GMT-14'')'));

-- DateTime64 -> Time64 (equal-scale path)
SELECT CAST(reinterpret(CAST(-9223372036854775808 AS Int64), 'DateTime64(0, ''Etc/GMT+12'')') AS Time64(0));

-- DateTime64 -> Time64 scale-changing path: rescaling the full epoch value first threw
-- DECIMAL_OVERFLOW for extreme inputs before the day-modulo ran. Split-then-rescale avoids it.
SELECT CAST(reinterpret(CAST(-9223372036854775808 AS Int64), 'DateTime64(0, ''Etc/GMT+12'')') AS Time64(9));
SELECT CAST(reinterpret(CAST(9223372036854775807 AS Int64), 'DateTime64(0, ''Etc/GMT-14'')') AS Time64(9));

-- Pre-epoch fractional values must land on the correct wall-clock day. Truncate-toward-zero
-- produced a negative Time64 on the wrong day; floor-aligned split keeps the fraction positive.
SELECT CAST(toDateTime64('1969-12-31 23:59:59.250', 3, 'Etc/GMT+0') AS Time64(3));
SELECT CAST(toDateTime64('1969-12-31 23:59:59.250', 3, 'Etc/GMT+0') AS Time64(9));
SELECT CAST(toDateTime64('1969-12-31 23:59:59.5', 1, 'Etc/GMT+0') AS Time64(9));

-- Sanity: ordinary values keep the expected wall-clock time-of-day.
SELECT toTime(toDateTime64('2024-06-15 14:30:17', 0, 'Etc/GMT+3'));
SELECT toTime(toDateTime64('2024-06-15 14:30:17', 0, 'Etc/GMT-3'));
