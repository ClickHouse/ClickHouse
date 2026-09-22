-- An accurate cast of `DateTime64` to `Time64` projects the value to the seconds of the local day, which always
-- fits the target, so the only inexact case is a reduction of the scale that drops a part of the fraction.
-- `CAST` truncates it; `accurateCast` must throw, `accurateCastOrNull` must return NULL,
-- and `accurateCastOrDefault` must return the default.

SET session_timezone = 'UTC';

-- Same scale, wider scale and an exactly representable narrower scale are fine.
SELECT accurateCast(toDateTime64('1970-01-01 12:34:56.789', 3, 'UTC'), 'Time64(3)');
SELECT accurateCast(toDateTime64('1970-01-01 12:34:56.789', 3, 'UTC'), 'Time64(6)');
SELECT accurateCast(toDateTime64('1970-01-01 12:34:56.000', 3, 'UTC'), 'Time64(0)');
SELECT accurateCast(toDateTime64('1970-01-01 12:34:56.780', 3, 'UTC'), 'Time64(2)');

-- A lossy scale reduction.
SELECT CAST(toDateTime64('1970-01-01 12:34:56.789', 3, 'UTC'), 'Time64(0)');
SELECT accurateCast(toDateTime64('1970-01-01 12:34:56.789', 3, 'UTC'), 'Time64(0)'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(toDateTime64('1970-01-01 12:34:56.789', 3, 'UTC'), 'Time64(2)'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCastOrNull(toDateTime64('1970-01-01 12:34:56.789', 3, 'UTC'), 'Time64(0)');
SELECT accurateCastOrNull(toDateTime64('1970-01-01 12:34:56.789', 3, 'UTC'), 'Time64(2)');
SELECT accurateCastOrNull(toDateTime64('1970-01-01 12:34:56.789', 3, 'UTC'), 'Time64(3)');
SELECT accurateCastOrDefault(toDateTime64('1970-01-01 12:34:56.789', 3, 'UTC'), 'Time64(0)', toTime64('01:02:03', 0));
SELECT accurateCastOrDefault(toDateTime64('1970-01-01 12:34:56.000', 3, 'UTC'), 'Time64(0)', toTime64('01:02:03', 0));

-- The projection uses the time zone of the source, and a value far from the epoch still fits the target.
SELECT accurateCast(toDateTime64('2024-03-15 23:30:00.5', 1, 'Asia/Tokyo'), 'Time64(1)');
SELECT accurateCast(toDateTime64('2024-03-15 23:30:00.5', 1, 'Asia/Tokyo'), 'Time64(0)'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(toDateTime64('2024-03-15 23:30:00.5', 1, 'Asia/Tokyo'), 'Time64(9)');
SELECT accurateCastOrNull(toDateTime64('2262-04-11 23:47:16.854775807', 9, 'UTC'), 'Time64(9)');
SELECT accurateCastOrNull(toDateTime64('2262-04-11 23:47:16.854775807', 9, 'UTC'), 'Time64(0)');

-- The non-constant path, row by row.
SELECT accurateCastOrNull(materialize(dt), 'Time64(1)'), accurateCastOrDefault(materialize(dt), 'Time64(1)', toTime64('00:00:00', 1))
FROM values('dt DateTime64(2, \'UTC\')', ('2024-03-15 01:02:03.40'), ('2024-03-15 01:02:03.45'), ('2024-03-15 01:02:04.00'));
SELECT accurateCast(materialize(dt), 'Time64(1)') FROM values('dt DateTime64(2, \'UTC\')', ('2024-03-15 01:02:03.40'), ('2024-03-15 01:02:03.45')); -- { serverError CANNOT_CONVERT_TYPE }
