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

-- The same branch handles conversion of BFloat16 to Time: saturate to the Time range.
SET use_legacy_to_time = 0;
SELECT toTime(CAST(1e10 AS BFloat16));
SELECT toTime(CAST(-1e10 AS BFloat16));
