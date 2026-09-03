SET enable_time_time64_type=1;
-- Downscale (6 -> 3), exact millisecond boundary
SELECT toString(CAST(toTime64('01:02:03.123000', 6) AS Time64(3)));

-- Upscale (3 -> 6)
SELECT toString(CAST(toTime64('01:02:03.123', 3) AS Time64(6)));

-- Type names of casts
SELECT toTypeName(CAST(toTime64('01:02:03.123000', 6) AS Time64(3)));
SELECT toTypeName(CAST(toTime64('01:02:03.123', 3) AS Time64(6)));

-- Value equality on exact boundaries
SELECT CAST(toTime64('01:02:03.123000', 6) AS Time64(3)) = toTime64('01:02:03.123', 3);
SELECT CAST(toTime64('01:02:03.123', 3) AS Time64(6)) = toTime64('01:02:03.123000', 6);
