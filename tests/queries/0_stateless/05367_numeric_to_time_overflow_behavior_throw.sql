-- `date_time_overflow_behavior` for a signed or floating-point source of `Time` and for a number below the `Date32` range.

SET enable_time_time64_type = 1;

SET date_time_overflow_behavior = 'throw';
SELECT CAST(-4000000::Int64 AS Time); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(4000000::Int32 AS Time); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(-4000000::Int128 AS Time); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(4000000.5::Float64 AS Time); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(materialize(-4e6::Float32) AS Time); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(-3599999::Int64 AS Time), CAST(3599999::Int32 AS Time), CAST(-1.5::Float64 AS Time);
SELECT CAST(-1000000::Int64 AS Date32); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(-1e30::Float64 AS Date32); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(nan AS Date32); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SET date_time_overflow_behavior = 'saturate';
SELECT CAST(-4000000::Int64 AS Time), CAST(4000000::Int32 AS Time), CAST(4000000.5::Float64 AS Time), CAST(-1000000::Int64 AS Date32);
