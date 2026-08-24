-- `saturate` used to clamp to the whole second and drop the subsecond part, landing below the type maximum.
-- The checks below do not assume where the boundary sits, so they hold whatever the supported date range is.
SET session_timezone = 'UTC';
SET date_time_overflow_behavior = 'saturate';
SET allow_experimental_time_time64_type = 1;

SELECT 'DateTime64 scale 9 saturates to the largest tick';
-- At scale 9 the bound is the Int64 tick range, so the maximum is exactly Int64 max ticks.
SELECT toUnixTimestamp64Nano(toDateTime64(253402300800, 9)) = 9223372036854775807;
SELECT toUnixTimestamp64Nano(toDateTime64(253402300800::Int64, 9)) = 9223372036854775807;
SELECT toUnixTimestamp64Nano(toDateTime64(2.534023008e11, 9)) = 9223372036854775807;
SELECT toUnixTimestamp64Nano(toDateTime64(toDate32('2299-12-31'), 9)) = 9223372036854775807;

SELECT 'DateTime64 scale 3 and 6 keep a maximal subsecond part';
SELECT toUnixTimestamp64Milli(toDateTime64(253402300800, 3)) % 1000 = 999;
SELECT toUnixTimestamp64Micro(toDateTime64(253402300800, 6)) % 1000000 = 999999;
SELECT toUnixTimestamp64Micro(toDateTime64(253402300800::Int64, 6)) % 1000000 = 999999;

SELECT 'Time64 saturates to the largest tick';
SELECT toTime64(3600000, 6), toTime64(3600000::Int64, 6), toTime64(3600000.0, 6);
SELECT toTime64(3600000, 9);

SELECT 'in-range values are untouched';
SELECT toDateTime64(1, 9), toDateTime64(1::Int64, 6), toTime64(1, 6);

SELECT 'underflow still clamps to the lower bound';
SELECT toDateTime64(-1e30, 6), toTime64(-3600000, 6), toTime64(-3600000::Int64, 6);

