-- `saturate` used to clamp to the whole second and drop the subsecond part, landing below the type maximum
SET session_timezone = 'UTC';
SET date_time_overflow_behavior = 'saturate';
SET allow_experimental_time_time64_type = 1;

SELECT 'DateTime64 scale 9 saturates to the largest tick';
-- At scale 9 the bound is the Int64 tick range, so the maximum is exactly Int64 max ticks
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

SELECT 'underflow clamps to the smallest tick too';
-- At scale 6 the bound is the calendar range, whose minimum starts exactly at a second
SELECT toDateTime64(-1e30, 6), toDateTime64(-300000000000::Int64, 6);
SELECT toUnixTimestamp64Nano(toDateTime64(-1e30, 9)) = toInt64('-9223372036854775808'),
       toUnixTimestamp64Nano(toDateTime64(-300000000000::Int64, 9)) = toInt64('-9223372036854775808');
SELECT toTime64(-3600000, 6), toTime64(-3600000::Int64, 6), toTime64(-3600000.0, 6), toTime64(-3600000::Int64, 9);

SELECT 'numeric sources still saturate under throw, the transforms are dispatched with Ignore';
SELECT toTime64(materialize(3600000.0), 6), toTime64(materialize(3600000::Int64), 6), toTime64(materialize(3600000::UInt64), 6)
SETTINGS date_time_overflow_behavior = 'throw';

SELECT 'a fraction inside the boundary second survives, only real overflow saturates';
SELECT toDateTime64(9223372036.5, 9), toDateTime64(-9223372036.5, 9);
SELECT toTime64(3599999.5, 6), toTime64(-3599999.5, 6);
SELECT toTime64(3600000.5, 6), toTime64(-3600000.5, 6);

SELECT 'wide integer carriers saturate like the narrow ones';
SELECT toTime64(toUInt128(3600000), 6), toTime64(toInt128(3600000), 6), toTime64(toUInt256(3600000), 6), toTime64(toInt256(-3600000), 6);
SELECT toDateTime64(toUInt128(300000000000), 9), toDateTime64(toInt128(300000000000), 9), toDateTime64(toInt256(-300000000000), 9);
SELECT toTime64(toUInt128(3600000), 6) = toTime64(toUInt64(3600000), 6), toTime64(toUInt128(1), 6), toDateTime64(toUInt128(1), 9);

SELECT 'a wide source beyond Int64 keeps its sign instead of narrowing first';
SELECT toTime64(toInt128('9223372036854775808'), 6), toTime64(toInt128('-9223372036854775809'), 6);
SELECT toTime64(toInt256('9223372036854775808'), 6), toTime64(toUInt128('9223372036854775808'), 6);
SELECT toDateTime64(toInt128('9223372036854775808'), 9), toDateTime64(toInt128('-9223372036854775809'), 9);

SELECT 'accurate casts saturate the same way, wide carriers still reject';
-- Narrow carriers reach the saturating transform, so accurate casts clamp rather than reject (as they already did for Int32)
SELECT accurateCast(toUInt32(3600000), 'Time64(6)'), accurateCastOrNull(toUInt32(3600000), 'Time64(6)'), accurateCastOrNull(toInt32(3600000), 'Time64(6)');
SELECT accurateCastOrNull(toDate32('2299-12-31'), 'DateTime64(9, \'UTC\')');
-- Wide carriers keep the accurate gate and report instead of saturating
SELECT accurateCastOrNull(toUInt128('9223372036854775808'), 'DateTime64(9)'), accurateCastOrNull(toInt128('9223372036854775808'), 'DateTime64(9)'), accurateCastOrNull(toUInt128(3600000), 'Time64(6)');
