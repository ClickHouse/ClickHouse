SET enable_time_time64_type = 1;
SELECT toDateTime64('2025-11-18 20:25:52', 3)::Time;
SELECT toDateTime64('2025-11-18 20:25:52', 3)::Time64;
SELECT (toDateTime64('2025-11-18 20:25:52', 3)::Time)::Time64;
