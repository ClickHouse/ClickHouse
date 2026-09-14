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
-- to_utc_timestamp must floor too: the underlying floor second 05:59:59 is -04:00, so the result
-- is 09:59:59.999, not 10:59:59.999 (the -05:00 ceil second). This input straddles the boundary;
-- the earlier 01:59:59.999 case did not and would pass even without flooring this direction.
SELECT to_utc_timestamp(toDateTime64('1969-10-26 05:59:59.999', 3), 'America/New_York');
-- Positive offset direction: Europe/Paris changes +02:00 -> +01:00 at 1945-09-16 01:00:00 UTC,
-- so the floor second 00:59:59 UTC is still +02:00 and from_utc_timestamp must return 02:59:59.999.
SELECT from_utc_timestamp(toDateTime64('1945-09-16 00:59:59.999', 3), 'Europe/Paris');
SELECT to_utc_timestamp(toDateTime64('1945-09-16 00:59:59.999', 3), 'Europe/Paris');

-- Same floor-second split invariant in the DateTime64 -> Time64 conversion path: for a negative
-- fractional underlying value truncated division picks the ceil second, so timezoneOffset() can
-- read the wrong side of an offset boundary. The DateTime64 is typed with the target timezone and
-- holds a UTC instant; reinterpret() reaches the exact underlying value. America/New_York is
-- -05:00 at the floor second 1969-04-27 06:59:59 UTC, so toTime64 must return 01:59:59.999,
-- not 02:59:59.999 (the -04:00 next second). Positive direction: Europe/Paris is +02:00 at the
-- floor second 1945-09-16 00:59:59 UTC, so toTime64 must return 02:59:59.999.
SELECT toTime64(reinterpret(toDateTime64('1969-04-27 06:59:59.999', 3, 'UTC'), 'DateTime64(3, \'America/New_York\')'), 3);
SELECT toTime64(reinterpret(toDateTime64('1945-09-16 00:59:59.999', 3, 'UTC'), 'DateTime64(3, \'Europe/Paris\')'), 3);
-- The split must happen at the source scale: a lower output scale must not truncate the negative
-- fraction across the second boundary and move the instant onto the next (wrong-offset) second.
SELECT toTime64(reinterpret(toDateTime64('1969-04-27 06:59:59.999', 3, 'UTC'), 'DateTime64(3, \'America/New_York\')'), 2);
SELECT toTime64(reinterpret(toDateTime64('1969-04-27 06:59:59.999', 3, 'UTC'), 'DateTime64(3, \'America/New_York\')'), 0);
SELECT toTime64(reinterpret(toDateTime64('1945-09-16 00:59:59.999', 3, 'UTC'), 'DateTime64(3, \'Europe/Paris\')'), 0);
SELECT toTime64(reinterpret(toDateTime64('1969-04-27 06:59:59.999', 3, 'UTC'), 'DateTime64(3, \'America/New_York\')'), 6);
