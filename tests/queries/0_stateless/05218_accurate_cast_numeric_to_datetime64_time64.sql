-- `accurateCast`, `accurateCastOrNull` and `accurateCastOrDefault` from a number to `DateTime64` / `Time64` decide
-- on representability alone: the setting `date_time_overflow_behavior` governs only the ordinary `CAST`, which
-- saturates (`ignore`, `saturate`) or throws (`throw`). The checks below run under every mode.

SELECT '-- accurateCast rejects an out-of-range value under every mode';
SELECT accurateCast(999999999999::UInt64, 'DateTime64(0, \'UTC\')') SETTINGS date_time_overflow_behavior = 'ignore'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT accurateCast(999999999999::UInt64, 'DateTime64(0, \'UTC\')') SETTINGS date_time_overflow_behavior = 'saturate'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT accurateCast(999999999999::UInt64, 'DateTime64(0, \'UTC\')') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT accurateCast(-999999999999::Int64, 'DateTime64(3, \'UTC\')') SETTINGS date_time_overflow_behavior = 'ignore'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT accurateCast(999999999999.0::Float64, 'DateTime64(3, \'UTC\')') SETTINGS date_time_overflow_behavior = 'saturate'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT accurateCast(999999999999::Int128, 'DateTime64(3, \'UTC\')') SETTINGS date_time_overflow_behavior = 'ignore'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT accurateCast(3600000::UInt64, 'Time64(0)') SETTINGS date_time_overflow_behavior = 'ignore'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT accurateCast(3600000::UInt32, 'Time64(3)') SETTINGS date_time_overflow_behavior = 'saturate'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT accurateCast(-3600000::Int64, 'Time64(3)') SETTINGS date_time_overflow_behavior = 'ignore'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT accurateCast(3600000.0::Float64, 'Time64(3)') SETTINGS date_time_overflow_behavior = 'ignore'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT accurateCast(3600000::UInt256, 'Time64(3)') SETTINGS date_time_overflow_behavior = 'ignore'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT '-- the same values are accepted by the ordinary CAST, which follows the setting';
SELECT CAST(999999999999::UInt64, 'DateTime64(0, \'UTC\')') SETTINGS date_time_overflow_behavior = 'ignore';
SELECT CAST(3600000::UInt64, 'Time64(0)') SETTINGS date_time_overflow_behavior = 'saturate';

SELECT '-- accurateCast keeps in-range values, including the fraction of the boundary second';
SELECT accurateCast(1700000000::UInt64, 'DateTime64(3, \'UTC\')') SETTINGS date_time_overflow_behavior = 'throw';
SELECT accurateCast(253402300799::Int64, 'DateTime64(0, \'UTC\')') SETTINGS date_time_overflow_behavior = 'throw';
SELECT accurateCast(253402300799.9::Float64, 'DateTime64(1, \'UTC\')') SETTINGS date_time_overflow_behavior = 'throw';
SELECT accurateCast(3599999.9::Float64, 'Time64(1)') SETTINGS date_time_overflow_behavior = 'throw';
SELECT accurateCast(-3599999::Int32, 'Time64(0)') SETTINGS date_time_overflow_behavior = 'throw';

SELECT '-- accurateCastOrNull returns NULL for an out-of-range value under every mode and never throws';
SELECT accurateCastOrNull(999999999999::UInt64, 'DateTime64(0, \'UTC\')') SETTINGS date_time_overflow_behavior = 'ignore';
SELECT accurateCastOrNull(999999999999::UInt64, 'DateTime64(0, \'UTC\')') SETTINGS date_time_overflow_behavior = 'saturate';
SELECT accurateCastOrNull(999999999999::UInt64, 'DateTime64(0, \'UTC\')') SETTINGS date_time_overflow_behavior = 'throw';
SELECT accurateCastOrNull(-999999999999::Int64, 'DateTime64(3, \'UTC\')') SETTINGS date_time_overflow_behavior = 'throw';
SELECT accurateCastOrNull(999999999999.0::Float64, 'DateTime64(3, \'UTC\')') SETTINGS date_time_overflow_behavior = 'throw';
SELECT accurateCastOrNull(nan::Float64, 'DateTime64(3, \'UTC\')') SETTINGS date_time_overflow_behavior = 'throw';
SELECT accurateCastOrNull(999999999999::UInt256, 'DateTime64(3, \'UTC\')') SETTINGS date_time_overflow_behavior = 'throw';
SELECT accurateCastOrNull(3600000::UInt64, 'Time64(0)') SETTINGS date_time_overflow_behavior = 'throw';
SELECT accurateCastOrNull(-3600000::Int64, 'Time64(3)') SETTINGS date_time_overflow_behavior = 'throw';
SELECT accurateCastOrNull(3600000.0::Float64, 'Time64(3)') SETTINGS date_time_overflow_behavior = 'throw';
SELECT accurateCastOrNull(-3600000::Int128, 'Time64(3)') SETTINGS date_time_overflow_behavior = 'throw';

SELECT '-- and keeps in-range values, including the fraction of the boundary second';
SELECT accurateCastOrNull(1700000000::UInt64, 'DateTime64(3, \'UTC\')') SETTINGS date_time_overflow_behavior = 'throw';
SELECT accurateCastOrNull(253402300799.9::Float64, 'DateTime64(1, \'UTC\')') SETTINGS date_time_overflow_behavior = 'throw';
SELECT accurateCastOrNull(-62167219200::Int64, 'DateTime64(3, \'UTC\')') SETTINGS date_time_overflow_behavior = 'throw';
SELECT accurateCastOrNull(3599999.9::Float64, 'Time64(1)') SETTINGS date_time_overflow_behavior = 'throw';
SELECT accurateCastOrNull(-100::Int8, 'Time64(0)') SETTINGS date_time_overflow_behavior = 'throw';

SELECT '-- the scale-dependent Int64 bound of DateTime64 is respected too';
SELECT accurateCastOrNull(9223372037::Int64, 'DateTime64(9, \'UTC\')') SETTINGS date_time_overflow_behavior = 'ignore';
SELECT accurateCastOrNull(9223372036::Int64, 'DateTime64(9, \'UTC\')') SETTINGS date_time_overflow_behavior = 'ignore';
SELECT accurateCast(9223372037::Int64, 'DateTime64(9, \'UTC\')') SETTINGS date_time_overflow_behavior = 'ignore'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT '-- accurateCastOrDefault follows accurateCastOrNull';
SELECT accurateCastOrDefault(999999999999::UInt64, 'DateTime64(0, \'UTC\')') SETTINGS date_time_overflow_behavior = 'throw';
SELECT accurateCastOrDefault(999999999999::UInt64, 'DateTime64(0, \'UTC\')', toDateTime64(1, 0, 'UTC')) SETTINGS date_time_overflow_behavior = 'throw';
SELECT accurateCastOrDefault(3600000::UInt64, 'Time64(0)') SETTINGS date_time_overflow_behavior = 'throw';

SELECT '-- non-constant columns';
SELECT accurateCastOrNull(x, 'DateTime64(0, \'UTC\')'), accurateCastOrNull(x, 'Time64(0)')
FROM (SELECT arrayJoin([0::Int64, 3599999, 3600000, 253402300799, 253402300800, -1]) AS x)
SETTINGS date_time_overflow_behavior = 'throw';
