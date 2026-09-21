-- `saturate` used to clamp to the whole second and drop the subsecond part, landing below the type maximum.
-- On this branch the range ends at 2299-12-31, and at scale 9 not even that fits into the Int64 ticks
SET session_timezone = 'UTC';
SET date_time_overflow_behavior = 'saturate';
SET allow_experimental_time_time64_type = 1;

SELECT 'DateTime64 scale 3 and 6 saturate to the last tick of 2299-12-31';
SELECT toDateTime64(10413792000, 3), toDateTime64(10413792000::Int64, 3), toDateTime64(1e30, 3);
SELECT toDateTime64(10413792000, 6), toDateTime64(10413792000::Int64, 6), toDateTime64(1e30, 6);

SELECT 'DateTime64 scale 9 saturates to the largest Int64 tick instead of overflowing';
SELECT toDateTime64(10413792000, 9), toDateTime64(10413792000::Int64, 9), toDateTime64(1e30, 9);
-- 2262-04-11 23:47:17 is inside the calendar range but past what the ticks can hold, so it saturates too
SELECT toDateTime64(9223372037, 9), toDateTime64(9223372037::Int64, 9), toDateTime64(9223372037.0, 9);
SELECT toUnixTimestamp64Nano(toDateTime64(1e30, 9)) = 9223372036854775807;

SELECT 'Time64 saturates to the largest tick';
SELECT toTime64(3600000, 6), toTime64(3600000::Int64, 6), toTime64(3600000.0, 6), toTime64(3600000::UInt32, 6);
SELECT toTime64(3600000, 9);

SELECT 'in-range values are untouched';
SELECT toDateTime64(1, 9), toDateTime64(1::Int64, 6), toTime64(1, 6), toDateTime64(9223372036, 9);

SELECT 'underflow clamps to the smallest tick too';
SELECT toDateTime64(-1e30, 6), toDateTime64(-300000000000::Int64, 6), toDateTime64(-1e30, 9), toDateTime64(-300000000000::Int64, 9);
SELECT toTime64(-3600000, 6), toTime64(-3600000::Int64, 6), toTime64(-3600000.0, 6), toTime64(-3600000::Int64, 9);

SELECT 'a fraction inside the boundary second survives, only real overflow saturates';
SELECT toDateTime64(10413791999.5, 3), toDateTime64(-2208988799.5, 3), toDateTime64(9223372036.5, 9);
SELECT toTime64(3599999.5, 6), toTime64(-3599999.5, 6);
SELECT toTime64(3600000.5, 6), toTime64(-3600000.5, 6);

SELECT 'saturated values compare equal whatever carried them';
SELECT toTime64(3600000::UInt32, 6) = toTime64(3600000::UInt64, 6), toTime64(99999999::Int64, 6) = toTime64(3600000.0, 6), toDateTime64(1e30, 9) = toDateTime64(99999999999::Int64, 9);

SELECT 'numeric sources still saturate under throw, the transforms are dispatched with Ignore';
SELECT toTime64(materialize(3600000.0), 6), toTime64(materialize(3600000::Int64), 6), toTime64(materialize(3600000::UInt64), 6)
SETTINGS date_time_overflow_behavior = 'throw';
