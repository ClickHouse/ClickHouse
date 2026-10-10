-- `DateTime64` ticks are stored in an `Int64`, so at scale 9 the calendar minimum (0000-01-01) is not
-- representable and the range bottoms out at `Int64::min` = -9223372036.854775808 seconds. The whole-second
-- lower bound truncates towards zero to -9223372036, so the last 0.854775808 seconds of the range used to be
-- rejected by `throw` and skipped by `saturate`. The upper side has always accounted for the fraction.

-- The exact native value depends on the binary rounding of the scaled `Float64` product, so the checks only
-- assert that it lands strictly below the minimum whole second and no lower than the minimum tick.
SELECT '-- a negative boundary value whose fraction is still representable is neither rejected nor clamped';
SELECT reinterpretAsInt64(toDateTime64(-9223372036.5, 9, 'UTC')) BETWEEN -9223372036854775808 AND -9223372036000000001
SETTINGS date_time_overflow_behavior = 'throw';

SELECT '-- and it survives the other overflow modes unchanged';
SELECT reinterpretAsInt64(toDateTime64(-9223372036.5, 9, 'UTC')) BETWEEN -9223372036854775808 AND -9223372036000000001
SETTINGS date_time_overflow_behavior = 'saturate';
SELECT reinterpretAsInt64(toDateTime64(-9223372036.5, 9, 'UTC')) BETWEEN -9223372036854775808 AND -9223372036000000001
SETTINGS date_time_overflow_behavior = 'ignore';

SELECT '-- saturate clamps a below-range value to the minimum tick, not to the minimum whole second';
SELECT reinterpretAsInt64(toDateTime64(-1e20, 9, 'UTC')) = -9223372036854775808,
       reinterpretAsInt64(toDateTime64(-300000000000, 9, 'UTC')) = -9223372036854775808,
       reinterpretAsInt64(toDateTime64(-inf, 9, 'UTC')) = -9223372036854775808
SETTINGS date_time_overflow_behavior = 'saturate';

SELECT '-- a value below the minimum tick is still rejected';
SELECT toDateTime64(-1e20, 9, 'UTC') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDateTime64(-300000000000, 9, 'UTC') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT '-- at a scale where the calendar minimum fits, the bound is the calendar minimum';
SELECT reinterpretAsInt64(toDateTime64(-1e20, 0, 'UTC')) = -62167219200,
       toString(toDateTime64(-1e20, 0, 'UTC')) = '0000-01-01 00:00:00'
SETTINGS date_time_overflow_behavior = 'saturate';

SELECT '-- numeric casts to Time honor date_time_overflow_behavior as the setting description says';
SELECT CAST(3600000 AS Time) SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(-3600000 AS Time) SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(3600000.5 AS Time) SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(3600000 AS Time) SETTINGS date_time_overflow_behavior = 'saturate';
SELECT CAST(3600000 AS Time) SETTINGS date_time_overflow_behavior = 'ignore';
SELECT CAST(3599999 AS Time) SETTINGS date_time_overflow_behavior = 'throw';
