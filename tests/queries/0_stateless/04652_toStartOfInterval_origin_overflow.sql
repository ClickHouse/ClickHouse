-- `toStartOfInterval(time, INTERVAL, origin)` computes `origin + offset`. Near `INT64_MIN` with a negative
-- offset this used to overflow silently (UBSan); now it throws `DECIMAL_OVERFLOW`.
-- `reinterpret` injects raw internal values because CAST clamps to the valid DateTime64 range.

-- Overflow: the interval start falls below `INT64_MIN`, must throw.
SELECT toStartOfInterval(reinterpret(toInt64(-9223372036854775800), 'DateTime64(9, \'America/New_York\')'), toIntervalHour(2), reinterpret(toInt64(-9223372036854775800), 'DateTime64(9, \'America/New_York\')')); -- { serverError DECIMAL_OVERFLOW }

-- A representable negative interval start must still be returned.
SELECT toStartOfInterval(toDateTime64('1969-12-31 23:30:00', 3, 'America/New_York'), toIntervalHour(2), toDateTime64('1969-12-31 22:00:00', 3, 'America/New_York'));
