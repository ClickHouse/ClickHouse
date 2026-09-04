-- Fractional literals are exact binary fractions (.5, .875) so the test does not depend on float parsing precision.
-- The documented ranges of `DateTime64` and `Time64` include the fractional tail of the terminal second
-- (`9999-12-31 23:59:59.999...` and `±999:59:59.999...`). A range check against whole seconds treated those
-- representable values as overflow: `throw` raised `VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE`, and `saturate` / `ignore`
-- clamped to `.000` instead of the exact boundary. The same whole-second cutoff also made same-type
-- `accurateCast` / `accurateCastOrNull` reject the last fractional second.

SET session_timezone = 'UTC';

SELECT '-- floating-point sources inside the last second convert in every mode';
SET date_time_overflow_behavior = 'throw';
SELECT toDateTime64(253402300799.5::Float64, 1), toDateTime64(253402300799.875::Float64, 3), toDateTime64(253402300799.5::Float64, 6);
SELECT toTime64(-3599999.5::Float64, 1), toTime64(3599999.875::Float64, 3), toTime64(-3599999.875::Float64, 6);
SELECT toDateTime64(x, 1), toTime64(y, 1) FROM (SELECT materialize(253402300799.5::Float64) AS x, materialize(-3599999.5::Float64) AS y);
SET date_time_overflow_behavior = 'saturate';
SELECT toDateTime64(253402300799.5::Float64, 1), toTime64(-3599999.5::Float64, 1);
SET date_time_overflow_behavior = 'ignore';
SELECT toDateTime64(253402300799.5::Float64, 1), toTime64(-3599999.5::Float64, 1);

SELECT '-- the first value past the tail is still an overflow';
SET date_time_overflow_behavior = 'throw';
SELECT toDateTime64(253402300800::Float64, 1); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDateTime64(-62167219200.5::Float64, 1); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toTime64(3600000::Float64, 1); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toTime64(-3600000::Float64, 1); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDateTime64(x, 1) FROM (SELECT materialize(253402300800::Float64) AS x); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT '-- saturation goes to the exact boundary, fractional tail included';
SET date_time_overflow_behavior = 'saturate';
SELECT toDateTime64(253402300800::Float64, 1), toDateTime64(1e300::Float64, 3), toDateTime64(1e30::Float32, 6);
SELECT toDateTime64(-62167219200.5::Float64, 1), toDateTime64(-1e300::Float64, 3);
SELECT toTime64(3600000::Float64, 1), toTime64(1e300::Float64, 3), toTime64(-1e30::Float32, 6);
SELECT toDateTime64(x, 3), toTime64(x, 3) FROM (SELECT materialize(1e300::Float64) AS x);
-- At scale 9 the `Int64` runs out before the year 9999: the boundary is the last representable tick.
SELECT toDateTime64(1e300::Float64, 9), toDateTime64(-1e300::Float64, 9);
SET date_time_overflow_behavior = 'ignore';
SELECT toDateTime64(253402300800::Float64, 1), toTime64(-3600000::Float64, 1);

SELECT '-- accurate casts accept the last fractional second, including the same-type round trip';
SET date_time_overflow_behavior = 'throw';
SELECT accurateCast(toDateTime64('9999-12-31 23:59:59.999', 3, 'UTC'), 'DateTime64(3)'), accurateCastOrNull(toDateTime64('9999-12-31 23:59:59.999', 3, 'UTC'), 'DateTime64(3)');
SELECT accurateCast(toTime64('999:59:59.999', 3), 'Time64(3)'), accurateCastOrNull(toTime64('-999:59:59.999', 3), 'Time64(3)');
SELECT accurateCast(toDateTime64('9999-12-31 23:59:59.999', 3, 'UTC'), 'DateTime64(6)'), accurateCastOrNull(toDateTime64('9999-12-31 23:59:59.999', 3, 'UTC'), 'DateTime64(1)');
SELECT accurateCast(253402300799.5::Float64, 'DateTime64(1)'), accurateCastOrNull(253402300799.5::Float64, 'DateTime64(1)');
SELECT accurateCast(-3599999.5::Float64, 'Time64(1)'), accurateCastOrNull(3599999.875::Float64, 'Time64(3)');
SELECT accurateCast(toDecimal64(253402300799.5, 1), 'DateTime64(1)'), accurateCastOrNull(toDecimal64(-3599999.5, 1), 'Time64(1)');
SELECT accurateCast(x, 'DateTime64(3)'), accurateCastOrNull(x, 'DateTime64(3)') FROM (SELECT materialize(toDateTime64('9999-12-31 23:59:59.999', 3, 'UTC')) AS x);

SELECT '-- and still reject the first value past it';
SELECT accurateCastOrNull(253402300800::Float64, 'DateTime64(1)'), accurateCastOrNull(3600000::Float64, 'Time64(1)');
SELECT accurateCastOrNull(toDecimal64(253402300800, 1), 'DateTime64(1)'), accurateCastOrNull(toDecimal64(-3600000, 1), 'Time64(1)');
-- `9999-12-31 23:59:59.999` has no scale-9 representation at all.
SELECT accurateCastOrNull(toDateTime64('9999-12-31 23:59:59.999', 3, 'UTC'), 'DateTime64(9)');
SELECT accurateCast(253402300800::Float64, 'DateTime64(1)'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT accurateCast(toDecimal64(253402300800, 1), 'DateTime64(1)'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT accurateCast(toDateTime64('9999-12-31 23:59:59.999', 3, 'UTC'), 'DateTime64(9)'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
