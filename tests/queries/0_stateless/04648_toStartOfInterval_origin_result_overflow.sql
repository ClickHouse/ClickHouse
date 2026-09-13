-- The three-argument overload of `toStartOfInterval` added the rounding offset back to the origin without an
-- overflow check, so for an origin near a bound of `Int64` the addition wrapped around: undefined behaviour
-- reported by the UBSan build as `signed integer overflow: -9223372036854775807 + -7200000000000`. Now it
-- throws `DECIMAL_OVERFLOW`.

-- Only the calendar units can still overshoot the time argument, by at most the time of day of the origin:
-- they round on the grid of the origin's local midnight, the fixed-length ones on the grid of the origin.
SELECT toStartOfInterval(reinterpret(toInt64(9223372036854775807), 'DateTime64(9, \'UTC\')'), toIntervalMonth(1), toDateTime64('2262-03-11 23:59:59', 9, 'UTC')); -- { serverError DECIMAL_OVERFLOW }

-- A zero-length difference must give back the origin. These used to wrap around instead, because the offset
-- came out negative in a time zone east of UTC: the difference, a duration, was rounded on the grid anchored
-- at the local midnight of the time point it is not.
SELECT toStartOfInterval(reinterpret(toInt64(-9223372036854775807), 'DateTime64(9, \'Asia/Istanbul\')'), toIntervalHour(3), reinterpret(toInt64(-9223372036854775807), 'DateTime64(9, \'Asia/Istanbul\')'));
SELECT toStartOfInterval(reinterpret(toInt64(-9223372036854775808), 'DateTime64(9, \'Europe/Berlin\')'), toIntervalHour(11), reinterpret(toInt64(-9223372036854775808), 'DateTime64(9, \'Europe/Berlin\')'));
SELECT toStartOfInterval(reinterpret(toInt64(-9223372036854775807), 'DateTime64(9, \'Asia/Kolkata\')'), toIntervalDay(2), reinterpret(toInt64(-9223372036854775807), 'DateTime64(9, \'Asia/Kolkata\')'));
SELECT toStartOfInterval(reinterpret(toInt64(-9223372036854775807), 'DateTime64(9, \'UTC\')'), toIntervalHour(3), reinterpret(toInt64(-9223372036854775807), 'DateTime64(9, \'UTC\')'));

-- Ordinary values must be unaffected.
SELECT toStartOfInterval(toDateTime64('2026-07-27 13:45:12.345', 3, 'UTC'), toIntervalHour(3), toDateTime64('2026-07-01 00:00:00.000', 3, 'UTC'));
SELECT toStartOfInterval(toDateTime64('2026-07-27 13:45:12.345', 3, 'Asia/Istanbul'), toIntervalDay(2), toDateTime64('2026-07-01 00:00:00.000', 3, 'Asia/Istanbul'));
SELECT toStartOfInterval(toDateTime('2026-07-27 13:45:12', 'Asia/Istanbul'), toIntervalMonth(1), toDateTime('2026-01-15 00:00:00', 'Asia/Istanbul'));
SELECT toStartOfInterval(toDate('2026-07-27'), toIntervalWeek(2), toDate('2026-01-05'));
