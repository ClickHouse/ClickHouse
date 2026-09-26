-- `Time64(0)` is a whole-second target, so accurate casts must reject a fractional source.

SELECT accurateCastOrNull(toTime64('00:00:00.5', 1), 'Time64(0)');
SELECT accurateCast(toTime64('00:00:00.5', 1), 'Time64(0)'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCastOrDefault(toTime64('00:00:00.5', 1), 'Time64(0)', toTime64('01:02:03', 0));
SELECT accurateCast(toTime64('01:02:03', 0), 'Time64(0)');
