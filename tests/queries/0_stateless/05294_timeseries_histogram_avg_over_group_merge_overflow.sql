-- Test: merging two partial `timeSeriesHistogramAvgOverGroup` states whose running sums are finite but whose merged sum
-- overflows switches to the incremental mean, like adding the samples one by one does (see 05033_timeseries_histogram_math_operators).

SET enable_nullable_tuple_type = 1;
SET allow_experimental_time_series_aggregate_functions = 1;

SELECT '-- two partial states of one sample each with count = sum = bucket = 1.5e308: the average stays finite (1.5e308)';
SELECT timeSeriesHistogramAvgOverGroupMerge(state)
FROM
(
    SELECT timeSeriesHistogramAvgOverGroupState(h) AS state FROM (SELECT (0, 0, 0., 1.5e308, 1.5e308, 0., [(0, 1)], [1.5e308], [], [], [], 0, 0, [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64), count_int UInt64, zero_count_int UInt64, positive_values_int Array(UInt64), negative_values_int Array(UInt64)) AS h)
    UNION ALL
    SELECT timeSeriesHistogramAvgOverGroupState(h) AS state FROM (SELECT (0, 0, 0., 1.5e308, 1.5e308, 0., [(0, 1)], [1.5e308], [], [], [], 0, 0, [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64), count_int UInt64, zero_count_int UInt64, positive_values_int Array(UInt64), negative_values_int Array(UInt64)) AS h)
);
