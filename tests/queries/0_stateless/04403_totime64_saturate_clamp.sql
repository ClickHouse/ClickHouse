SET allow_experimental_time_time64_type = 1;

-- Int64 -> Time64: values out of range must be clamped in saturate mode (they used to be stored unclamped,
-- yielding Time64 values that display identically but compare as different).
-- Two different out-of-range inputs must land on the same value
SELECT CAST(9999999::Int64, 'Time64') = CAST(8888888::Int64, 'Time64') SETTINGS date_time_overflow_behavior = 'saturate';
SELECT toInt64(CAST(9999999::Int64, 'Time64')) SETTINGS date_time_overflow_behavior = 'saturate';

-- Float64 -> Time64: same issue (it clamped to the much wider DateTime64 range).
SELECT CAST(9999999.0::Float64, 'Time64') = CAST(8888888.0::Float64, 'Time64') SETTINGS date_time_overflow_behavior = 'saturate';
SELECT toInt64(CAST(9999999.0::Float64, 'Time64')) SETTINGS date_time_overflow_behavior = 'saturate';

-- UInt64 -> Time64: baseline, already clamped correctly.
SELECT CAST(9999999::UInt64, 'Time64') = CAST(8888888::UInt64, 'Time64') SETTINGS date_time_overflow_behavior = 'saturate';
SELECT toInt64(CAST(9999999::UInt64, 'Time64')) SETTINGS date_time_overflow_behavior = 'saturate';

-- Negative values are clamped to the lower bound for signed and float inputs.
SELECT toInt64(CAST(-9999999::Int64, 'Time64')) SETTINGS date_time_overflow_behavior = 'saturate';
SELECT toInt64(CAST(-9999999.0::Float64, 'Time64')) SETTINGS date_time_overflow_behavior = 'saturate';
