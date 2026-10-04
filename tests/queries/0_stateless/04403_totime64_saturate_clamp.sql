SET allow_experimental_time_time64_type = 1;

-- Out-of-range values in `saturate` mode must be clamped to the maximum representable Time64 value including
-- its sub-second fraction (`999:59:59.999`), not stored unclamped. Two distinct out-of-range inputs therefore
-- collapse to the same clamped value and compare as equal; if either were stored raw (the original bug) they
-- would differ.

-- Int64 -> Time64.
SELECT CAST(9999999::Int64, 'Time64') = CAST(9999998::Int64, 'Time64') SETTINGS date_time_overflow_behavior = 'saturate';
SELECT toInt64(CAST(9999999::Int64, 'Time64')) SETTINGS date_time_overflow_behavior = 'saturate';

-- Float64 -> Time64 (it used to clamp to the much wider DateTime64 range).
SELECT CAST(9999999.0::Float64, 'Time64') = CAST(9999998.0::Float64, 'Time64') SETTINGS date_time_overflow_behavior = 'saturate';
SELECT toInt64(CAST(9999999.0::Float64, 'Time64')) SETTINGS date_time_overflow_behavior = 'saturate';

-- UInt64 -> Time64.
SELECT CAST(9999999::UInt64, 'Time64') = CAST(9999998::UInt64, 'Time64') SETTINGS date_time_overflow_behavior = 'saturate';
SELECT toInt64(CAST(9999999::UInt64, 'Time64')) SETTINGS date_time_overflow_behavior = 'saturate';

-- Negative values are clamped to the lower bound for signed and float inputs.
SELECT toInt64(CAST(-9999999::Int64, 'Time64')) SETTINGS date_time_overflow_behavior = 'saturate';
SELECT toInt64(CAST(-9999999.0::Float64, 'Time64')) SETTINGS date_time_overflow_behavior = 'saturate';
