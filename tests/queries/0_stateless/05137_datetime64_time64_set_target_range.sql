-- Set conversion must reject UInt64 values outside the target-specific range, even when they fit in Int64.

SELECT '-- DateTime64: the UInt64 bound depends on the scale';
SELECT toDateTime64('2000-01-01', 9, 'UTC') IN (toUInt64(300000000000));
SELECT toDateTime64('2000-01-01', 9, 'UTC') NOT IN (toUInt64(300000000000));
SELECT x FROM values('x DateTime64(9, ''UTC'')', toUInt64(300000000000)); -- { serverError ARGUMENT_OUT_OF_BOUND }

SET allow_experimental_time_time64_type = 1;

SELECT '-- Time64: the UInt64 bound is 999:59:59';
SELECT toTime64('00:00:01', 0) IN (toUInt64(3600000));
SELECT toTime64('00:00:01', 0) NOT IN (toUInt64(3600000));
SELECT x FROM values('x Time64(0)', toUInt64(3600000)); -- { serverError ARGUMENT_OUT_OF_BOUND }
