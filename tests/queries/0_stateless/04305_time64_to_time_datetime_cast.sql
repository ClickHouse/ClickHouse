-- Edge cases of the Time64 -> Time and Time64 -> DateTime casts (PR #106019 review).
-- The previous hand-written division truncated negative fractional values towards zero
-- (`-00:00:00.5` became `00:00:00` instead of `-00:00:01`) and bypassed the DateTime range
-- handling, so negative values silently wrapped through `UInt32` instead of being clamped
-- or rejected according to `date_time_overflow_behavior`.
-- Both `Time64` and `Time` are timezone-unaware, so a non-UTC session must not shift values.

SET session_timezone = 'Asia/Shanghai';

SELECT '-- Time64 -> Time: drop sub-second, flooring towards negative infinity --';
SELECT toString(CAST(toTime64('01:02:03.456', 3) AS Time));
SELECT toString(CAST(CAST(-0.5 AS Time64(1)) AS Time));
SELECT toString(CAST(CAST(-1.5 AS Time64(1)) AS Time));
SELECT toString(CAST(CAST(-1 AS Time64(3)) AS Time));

SELECT '-- Time64 -> DateTime: whole seconds reinterpreted as seconds since 1970-01-01 --';
SELECT toString(CAST(toTime64('01:02:03.456', 3) AS DateTime('UTC')));

SELECT '-- Time64 -> DateTime: negative value range handling honors date_time_overflow_behavior --';
SELECT toString(CAST(CAST(-1 AS Time64(0)) AS DateTime('UTC'))) SETTINGS date_time_overflow_behavior = 'saturate';
SELECT CAST(CAST(-1 AS Time64(0)) AS DateTime('UTC')) SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
