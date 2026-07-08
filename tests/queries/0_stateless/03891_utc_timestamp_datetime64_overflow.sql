-- Regression test for signed integer overflow (UBSan) in toUTCTimestamp / fromUTCTimestamp
-- on the DateTime64 path. Near-Int64::max/min underlying values overflowed the scale
-- multiply/add (and, for scale 0, the timezone-offset add/subtract). reinterpret() is used
-- to reach the raw boundary values that CAST would otherwise clamp before the function.
SET session_timezone = 'UTC';

SELECT toInt64(to_utc_timestamp(reinterpret(9223372036854775807, 'DateTime64(3)'), 'Asia/Shanghai')),
       toInt64(from_utc_timestamp(reinterpret(9223372036854775807, 'DateTime64(3)'), 'Asia/Shanghai'));
SELECT toInt64(to_utc_timestamp(reinterpret(-9223372036854775808, 'DateTime64(3)'), 'Asia/Shanghai')),
       toInt64(from_utc_timestamp(reinterpret(-9223372036854775808, 'DateTime64(3)'), 'Asia/Shanghai'));
SELECT toInt64(to_utc_timestamp(reinterpret(9223372036854775807, 'DateTime64(9)'), 'America/New_York')),
       toInt64(from_utc_timestamp(reinterpret(9223372036854775807, 'DateTime64(9)'), 'America/New_York'));
SELECT toInt64(to_utc_timestamp(reinterpret(9223372036854775807, 'DateTime64(0)'), 'EST')),
       toInt64(from_utc_timestamp(reinterpret(9223372036854775807, 'DateTime64(0)'), 'EST'));
SELECT toInt64(to_utc_timestamp(reinterpret(-9223372036854775808, 'DateTime64(0)'), 'EST')),
       toInt64(from_utc_timestamp(reinterpret(-9223372036854775808, 'DateTime64(0)'), 'EST'));

-- Sanity: ordinary in-range values are unaffected by the fix.
SELECT to_utc_timestamp(toDateTime64('2023-03-16 11:22:33.123', 3), 'Asia/Shanghai'),
       from_utc_timestamp(toDateTime64('2023-03-16 11:22:33.123', 3), 'Asia/Shanghai');

-- Negative fractional values near an offset boundary: the seconds split must floor toward the
-- past second so timezoneOffset() reads the second the instant actually falls in, not the next
-- one. America/New_York changes -04:00 -> -05:00 at 1969-10-26 06:00:00 UTC, so 05:59:59.999 UTC
-- is still -04:00 and from_utc_timestamp must return 01:59:59.999, not 00:59:59.999 (negative offset).
SELECT from_utc_timestamp(toDateTime64('1969-10-26 05:59:59.999', 3), 'America/New_York');
SELECT to_utc_timestamp(toDateTime64('1969-10-26 01:59:59.999', 3), 'America/New_York');
-- Positive offset direction: Europe/Paris changes +02:00 -> +01:00 at 1945-09-16 01:00:00 UTC,
-- so the floor second 00:59:59 UTC is still +02:00 and from_utc_timestamp must return 02:59:59.999.
SELECT from_utc_timestamp(toDateTime64('1945-09-16 00:59:59.999', 3), 'Europe/Paris');
