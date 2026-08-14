-- `Time64` to whole-second temporal types must keep the `accurateCast` contract:
-- fractional seconds and target-range violations cannot be represented exactly.

SET session_timezone = 'UTC';

SELECT accurateCast(toTime64('01:02:03', 0), 'Time'), accurateCastOrNull(toTime64('01:02:03', 0), 'Time');
SELECT accurateCast(toTime64('01:02:03', 0), 'DateTime'), accurateCastOrNull(toTime64('01:02:03', 0), 'DateTime');

SELECT accurateCastOrNull(toTime64('00:00:00.5', 1), 'Time');
SELECT accurateCastOrNull(toTime64('00:00:00.5', 1), 'DateTime');
SELECT accurateCast(toTime64('00:00:00.5', 1), 'Time'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(toTime64('00:00:00.5', 1), 'DateTime'); -- { serverError CANNOT_CONVERT_TYPE }

SELECT accurateCastOrNull(CAST(-1 AS Time64(0)), 'DateTime');
SELECT accurateCast(CAST(-1 AS Time64(0)), 'DateTime'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCastOrDefault(toTime64('00:00:00.5', 1), 'Time', toTime('01:02:03'));
