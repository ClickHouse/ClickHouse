-- The three-argument overload of `toStartOfInterval` added the rounding offset back to the origin
-- without an overflow check. For an origin near the lower bound of `Int64` and a large interval, the
-- offset is negative (the interval boundary in local time lies before the origin because of the time
-- zone offset), so the addition wrapped around: undefined behaviour reported by the UBSan build as
-- `signed integer overflow: -9223372036854775807 + -7200000000000`. Now it throws `DECIMAL_OVERFLOW`.

-- An origin at the lower bound of `Int64` in a time zone east of UTC, so that the offset is negative.
SELECT toStartOfInterval(reinterpret(toInt64(-9223372036854775807), 'DateTime64(9, \'Asia/Istanbul\')'), toIntervalHour(3), reinterpret(toInt64(-9223372036854775807), 'DateTime64(9, \'Asia/Istanbul\')')); -- { serverError DECIMAL_OVERFLOW }
SELECT toStartOfInterval(reinterpret(toInt64(-9223372036854775808), 'DateTime64(9, \'Europe/Berlin\')'), toIntervalHour(11), reinterpret(toInt64(-9223372036854775808), 'DateTime64(9, \'Europe/Berlin\')')); -- { serverError DECIMAL_OVERFLOW }
SELECT toStartOfInterval(reinterpret(toInt64(-9223372036854775807), 'DateTime64(9, \'Asia/Kolkata\')'), toIntervalDay(2), reinterpret(toInt64(-9223372036854775807), 'DateTime64(9, \'Asia/Kolkata\')')); -- { serverError DECIMAL_OVERFLOW }

-- The same origin in UTC has a zero offset, so it must keep working.
SELECT toStartOfInterval(reinterpret(toInt64(-9223372036854775807), 'DateTime64(9, \'UTC\')'), toIntervalHour(3), reinterpret(toInt64(-9223372036854775807), 'DateTime64(9, \'UTC\')'));

-- Ordinary values must be unaffected. These pin the behaviour as it is today; the results of the
-- `SECOND`, `HOUR` and `DAY` units of this overload are rounded on the grid anchored at the local
-- midnight (see `toStartOfHourInterval` and `toStartOfDayInterval`), not on the grid anchored at the
-- origin, so `result - origin` is not necessarily a multiple of the interval in a time zone whose
-- offset is not a whole number of intervals. That is a separate question from the overflow fixed
-- here, and if these buckets are ever made strictly periodic from the origin, the second reference
-- value below changes.
SELECT toStartOfInterval(toDateTime64('2026-07-27 13:45:12.345', 3, 'UTC'), toIntervalHour(3), toDateTime64('2026-07-01 00:00:00.000', 3, 'UTC'));
SELECT toStartOfInterval(toDateTime64('2026-07-27 13:45:12.345', 3, 'Asia/Istanbul'), toIntervalDay(2), toDateTime64('2026-07-01 00:00:00.000', 3, 'Asia/Istanbul'));
SELECT toStartOfInterval(toDateTime('2026-07-27 13:45:12', 'Asia/Istanbul'), toIntervalMonth(1), toDateTime('2026-01-15 00:00:00', 'Asia/Istanbul'));
SELECT toStartOfInterval(toDate('2026-07-27'), toIntervalWeek(2), toDate('2026-01-05'));
