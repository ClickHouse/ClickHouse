-- avg over Date/Time types converts the average back to the native integer type.
-- The conversion used to go through Float64, which is inexact above 2^53: for ticks near the
-- bounds of Int64 the rounded Float64 can land on 2^63 exactly, and the cast was undefined
-- behavior (caught by UBSan). The division must be computed exactly in integer space, and the
-- remaining Float64 conversions must saturate instead of overflowing.

-- Exactly at the bounds.
SELECT avg(x) FROM (SELECT fromUnixTimestamp64Nano(9223372036854775807, 'UTC') AS x FROM numbers(2));
SELECT avg(x) FROM (SELECT fromUnixTimestamp64Nano(-9223372036854775808, 'UTC') AS x FROM numbers(2));

-- Just inside the bounds: Float64(2^63 - 1024) == Float64(2^63), so a Float64-based conversion
-- cannot distinguish the last 1024 ticks; the exact value must be preserved.
SELECT avg(x) FROM (SELECT fromUnixTimestamp64Nano(9223372036854775806, 'UTC') AS x FROM numbers(2));
SELECT avg(x) FROM (SELECT fromUnixTimestamp64Nano(-9223372036854775807, 'UTC') AS x FROM numbers(2));

-- A tie at the boundary rounds half to even: avg = ...806.5 -> ...806.
SELECT avg(x) FROM (SELECT fromUnixTimestamp64Nano(if(number % 2 = 0, 9223372036854775807, 9223372036854775806), 'UTC') AS x FROM numbers(100));

-- Time64 goes through the same conversion.
SET enable_time_time64_type = 1;
SELECT avg(x) FROM (SELECT toTime64('999:59:59.999999999', 9) AS x FROM numbers(2));
SELECT avg(x) FROM (SELECT toTime64(if(number % 2 = 0, '999:59:59.999999999', '999:59:59.999999998'), 9) AS x FROM numbers(100));
