-- DateTime64 and Time64 store scaled Int64 values. A UInt64 value above Int64::max
-- cannot be represented by either type and must not be narrowed to a negative value
-- while building an exact IN set.

SELECT '-- DateTime64: UInt64 max must not match -1';
SELECT toDateTime64(-1, 0, 'UTC') IN (toUInt64(18446744073709551615));
SELECT toDateTime64(-1, 0, 'UTC') NOT IN (toUInt64(18446744073709551615));
SELECT toDateTime64(-1, 0, 'UTC') = toUInt64(18446744073709551615); -- { serverError DECIMAL_OVERFLOW }

SELECT '-- DateTime64: representable UInt64 values still match';
SELECT toDateTime64('1970-01-01 00:00:01', 0, 'UTC') IN (toUInt64(1));

SELECT '-- The VALUES path rejects an unrepresentable UInt64';
SELECT x FROM values('x DateTime64(0, ''UTC'')', toUInt64(18446744073709551615)); -- { serverError ARGUMENT_OUT_OF_BOUND }

SET allow_experimental_time_time64_type = 1;

SELECT '-- Time64: UInt64 max must not match -1';
SELECT toTime64('-00:00:01', 0) IN (toUInt64(18446744073709551615));
SELECT toTime64('-00:00:01', 0) NOT IN (toUInt64(18446744073709551615));

SELECT '-- Time64: representable UInt64 values still match';
SELECT toTime64('00:00:01', 0) IN (toUInt64(1));

SELECT '-- The Time64 VALUES path rejects an unrepresentable UInt64';
SELECT x FROM values('x Time64(0)', toUInt64(18446744073709551615)); -- { serverError ARGUMENT_OUT_OF_BOUND }
