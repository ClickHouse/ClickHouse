SET allow_experimental_time_series_aggregate_functions = 1;

-- Identical NaN bit patterns are treated as the same value, while different NaN bit patterns are changes.
SELECT
    timeSeriesChangesToGrid(120, 120, 0, 40)(
        [89, 101]::Array(UInt32),
        [reinterpretAsFloat32(toUInt32(2143289344)), reinterpretAsFloat32(toUInt32(2143289344))]::Array(Float32)
    ) AS same_nan_float32,
    timeSeriesChangesToGrid(120, 120, 0, 40)(
        [89, 101]::Array(UInt32),
        [reinterpretAsFloat32(toUInt32(2143289344)), reinterpretAsFloat32(toUInt32(2143289345))]::Array(Float32)
    ) AS different_nan_float32,
    timeSeriesChangesToGrid(120, 120, 0, 40)(
        [89, 101]::Array(UInt32),
        [reinterpretAsFloat64(toUInt64(9221120237041090560)), reinterpretAsFloat64(toUInt64(9221120237041090560))]::Array(Float64)
    ) AS same_nan_float64,
    timeSeriesChangesToGrid(120, 120, 0, 40)(
        [89, 101]::Array(UInt32),
        [reinterpretAsFloat64(toUInt64(9221120237041090560)), reinterpretAsFloat64(toUInt64(9221120237041090561))]::Array(Float64)
    ) AS different_nan_float64;

-- The same rule applies when the transition is merged across bucket summaries.
SELECT
    timeSeriesChangesToGrid(100, 120, 10, 40)(
        [89, 101]::Array(UInt32),
        [reinterpretAsFloat64(toUInt64(9221120237041090560)), reinterpretAsFloat64(toUInt64(9221120237041090560))]::Array(Float64)
    ) AS same_nan,
    timeSeriesChangesToGrid(100, 120, 10, 40)(
        [89, 101]::Array(UInt32),
        [reinterpretAsFloat64(toUInt64(9221120237041090560)), reinterpretAsFloat64(toUInt64(9221120237041090561))]::Array(Float64)
    ) AS different_nan;
