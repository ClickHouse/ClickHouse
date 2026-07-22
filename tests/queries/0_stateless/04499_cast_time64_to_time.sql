-- Casting Time64 to Time was never implemented and failed with CANNOT_CONVERT_TYPE.
SET enable_time_time64_type = 1;

-- Direct cast: Time64 -> Time keeps the seconds and drops the fractional part.
SELECT CAST(CAST('10:00:00' AS Time64) AS Time) = CAST('10:00:00' AS Time);
SELECT CAST(CAST('10:00:00.987' AS Time64(3)) AS Time) = CAST('10:00:00' AS Time);
SELECT toTypeName(CAST(CAST('10:00:00' AS Time64) AS Time));

-- accurateCastOrNull must also succeed and never null out a valid value.
SELECT accurateCastOrNull(CAST('01:02:03.5' AS Time64(1)), 'Time') = CAST('01:02:03' AS Time);

-- Out-of-range Time64 (reachable via reinterpret; numeric casts already clamp) must clamp the stored
-- value to the Time range, not just its text. reinterpretAsInt32 exposes the raw stored seconds.
SELECT reinterpretAsInt32(CAST(reinterpret(toInt64(3600001), 'Time64(0)') AS Time)) = 3599999;
SELECT reinterpretAsInt32(CAST(reinterpret(toInt64(-3600001), 'Time64(0)') AS Time)) = -3599999;
