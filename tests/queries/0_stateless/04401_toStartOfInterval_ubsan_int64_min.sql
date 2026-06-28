-- Regression test: the toStartOf*Interval family must not invoke UBSan signed
-- integer overflow on a DateTime64 near INT64_MIN. Surfaced by the AST fuzzer.
SET session_timezone = 'UTC';

-- Minute family.
SELECT toStartOfMinute(reinterpret(toInt64(-9223372036854775808), 'DateTime64(0)'));
SELECT toStartOfFiveMinutes(reinterpret(toInt64(-9223372036854775808), 'DateTime64(0)'));
SELECT toStartOfTenMinutes(reinterpret(toInt64(-9223372036854775808), 'DateTime64(0)'));
SELECT toStartOfFifteenMinutes(reinterpret(toInt64(-9223372036854775808), 'DateTime64(0)'));

-- Hour.
SELECT toStartOfHour(reinterpret(toInt64(-9223372036854775808), 'DateTime64(0)'));

-- toStartOfInterval routing to the minute, 1-hour and N-hour code paths.
SELECT toStartOfInterval(reinterpret(toInt64(-9223372036854775808), 'DateTime64(0)'), INTERVAL 5 MINUTE);
SELECT toStartOfInterval(reinterpret(toInt64(-9223372036854775808), 'DateTime64(0)'), INTERVAL 1 HOUR);
SELECT toStartOfInterval(reinterpret(toInt64(-9223372036854775808), 'DateTime64(0)'), INTERVAL 3 HOUR);
SELECT toStartOfInterval(reinterpret(toInt64(-9223372036854775808), 'DateTime64(0)'), INTERVAL 6 HOUR, 'Asia/Kolkata');

-- Normal positive inputs still round down to the expected interval start.
SELECT toStartOfFiveMinutes(toDateTime64('2026-05-20 10:17:33', 0, 'UTC'));
SELECT toStartOfHour(toDateTime64('2026-05-20 10:17:33', 0, 'UTC'));
SELECT toStartOfInterval(toDateTime64('2026-05-20 10:17:33', 0, 'UTC'), INTERVAL 3 HOUR);

-- Normal negative (pre-epoch) inputs still round toward negative infinity.
SELECT toStartOfFiveMinutes(toDateTime64('1969-12-31 23:52:33', 0, 'UTC'));
SELECT toStartOfHour(toDateTime64('1969-12-31 23:52:33', 0, 'UTC'));
