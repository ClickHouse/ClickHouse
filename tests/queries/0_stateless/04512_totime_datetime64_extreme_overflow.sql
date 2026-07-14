-- Regression test: converting an extreme DateTime64 (INT64_MIN) to Time / Time64 in a
-- timezone with a negative offset must not trigger UBSan signed-integer overflow at
-- FunctionsConversion.h (dt64 + offset). The result is the local seconds-of-day.

SET enable_time_time64_type = 1;

-- DateTime64 -> Time (ToTimeImpl::execute(Int64))
SELECT toTime(reinterpret(CAST(-9223372036854775808 AS Int64), 'DateTime64(0, ''America/New_York'')'));
SELECT toTime(reinterpret(CAST(9223372036854775807 AS Int64), 'DateTime64(0, ''America/New_York'')'));

-- DateTime64 -> Time64 (equal-scale path)
SELECT CAST(reinterpret(CAST(-9223372036854775808 AS Int64), 'DateTime64(0, ''America/New_York'')') AS Time64(0));

-- Sanity: ordinary values keep the expected wall-clock time-of-day.
SELECT toTime(toDateTime64('2024-06-15 14:30:17', 0, 'America/New_York'));
SELECT toTime(toDateTime64('2024-06-15 14:30:17', 0, 'Europe/Moscow'));
