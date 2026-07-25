-- Conversion of BFloat16 to DateTime must saturate to the range boundaries,
-- like Float32 and Float64 do, instead of relying on the platform-specific
-- result of an out-of-range float-to-integer cast (which wraps around on x86-64
-- but saturates on AArch64).

-- In-range value.
SELECT toDateTime32(CAST(1000000 AS BFloat16), 'UTC');
SELECT toDateTime(CAST(1000000 AS BFloat16), 'UTC');

-- Value above the DateTime range: saturate to the upper boundary.
SELECT toDateTime32(CAST(1e10 AS BFloat16), 'UTC');
SELECT toDateTime(CAST(1e10 AS BFloat16), 'UTC');

-- Negative value: saturate to the lower boundary.
SELECT toDateTime32(CAST(-100 AS BFloat16), 'UTC');
SELECT toDateTime(CAST(-100 AS BFloat16), 'UTC');

-- Numeric inputs saturate regardless of date_time_overflow_behavior.
SELECT toDateTime32(CAST(1e10 AS BFloat16), 'UTC') SETTINGS date_time_overflow_behavior = 'throw';
SELECT toDateTime32(CAST(-100 AS BFloat16), 'UTC') SETTINGS date_time_overflow_behavior = 'throw';

-- Very large finite values, above the range of time_t after the cast to Float64,
-- must also saturate instead of hitting an undefined float-to-integer cast.
SELECT toDateTime32(CAST(1e38 AS BFloat16), 'UTC');
SELECT toDateTime32(CAST(-1e38 AS BFloat16), 'UTC');
SELECT toDateTime32(CAST(1e300 AS Float64), 'UTC');
SELECT toDateTime32(CAST(-1e300 AS Float64), 'UTC');

-- Non-finite values cannot be converted and throw, like they do for integer targets.
SELECT toDateTime32(CAST('nan' AS BFloat16), 'UTC'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toDateTime32(CAST('inf' AS BFloat16), 'UTC'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toDateTime32(CAST('-inf' AS BFloat16), 'UTC'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toDateTime(nan, 'UTC'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toDateTime(CAST('inf' AS Float32), 'UTC'); -- { serverError CANNOT_CONVERT_TYPE }

-- The toDate float path follows the same contract: non-finite values throw,
-- huge finite values saturate to the upper boundary instead of hitting an
-- undefined float-to-integer cast.
SELECT toDate(CAST(100 AS BFloat16), 'UTC');
SELECT toDate(CAST(1e10 AS BFloat16), 'UTC');
SELECT toDate(CAST(1e38 AS BFloat16), 'UTC');
SELECT toDate(CAST(1e300 AS Float64), 'UTC');
SELECT toDate(CAST(-100 AS BFloat16), 'UTC');
SELECT toDate(CAST(1e300 AS Float64), 'UTC') SETTINGS date_time_overflow_behavior = 'throw';
SELECT toDate(CAST('nan' AS BFloat16), 'UTC'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toDate(CAST('inf' AS BFloat16), 'UTC'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toDate(CAST('-inf' AS BFloat16), 'UTC'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toDate(nan, 'UTC'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toDate(CAST('inf' AS Float32), 'UTC'); -- { serverError CANNOT_CONVERT_TYPE }

-- The same branch handles conversion of BFloat16 to Time: saturate to the Time range.
SET use_legacy_to_time = 0;
SELECT toTime(CAST(1e10 AS BFloat16));
SELECT toTime(CAST(-1e10 AS BFloat16));
SELECT toTime(CAST(1e38 AS BFloat16));
SELECT toTime(CAST(-1e38 AS BFloat16));
SELECT toTime(CAST('nan' AS BFloat16)); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toTime(CAST('inf' AS BFloat16)); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toTime(CAST('-inf' AS BFloat16)); -- { serverError CANNOT_CONVERT_TYPE }
