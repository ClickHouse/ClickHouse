-- Casting Time64 to Time was never implemented and failed with CANNOT_CONVERT_TYPE.
SET enable_time_time64_type = 1;

-- Direct cast: Time64 -> Time keeps the seconds and drops the fractional part.
SELECT CAST(CAST('10:00:00' AS Time64) AS Time) = CAST('10:00:00' AS Time);
SELECT CAST(CAST('10:00:00.987' AS Time64(3)) AS Time) = CAST('10:00:00' AS Time);
SELECT toTypeName(CAST(CAST('10:00:00' AS Time64) AS Time));

-- accurateCastOrNull must also succeed and never null out a valid value.
SELECT accurateCastOrNull(CAST('01:02:03.5' AS Time64(1)), 'Time') = CAST('01:02:03' AS Time);

-- Implicit conversion when reading back a Time column from Arrow, which stores it as Time64(0).
INSERT INTO FUNCTION file('04499_time64_to_time.arrow', Arrow, 'c1 Time') VALUES ('10:00:00');
SELECT c1 = CAST('10:00:00' AS Time) FROM file('04499_time64_to_time.arrow', Arrow, 'c1 Time');
