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
