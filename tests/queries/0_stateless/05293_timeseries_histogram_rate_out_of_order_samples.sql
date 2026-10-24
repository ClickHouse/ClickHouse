-- Test: the rate-family native-histogram aggregates keep all samples of a bucket in one slice of the state's blob.
-- When a bucket gets a sample after another bucket did, its slice is copied to the end of the blob;
-- that copy used to insert a range of the blob into itself (a logical error found by the AST fuzzer).
-- Both the add path (samples arriving out of bucket order) and the merge path (two partial states with samples
-- in the same buckets) must give the same result as the in-order aggregation in 05031_timeseries_histogram_rate_aggregates.

SET allow_experimental_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS hist_samples_out_of_order;
CREATE TABLE hist_samples_out_of_order
(
    timestamp UInt32,
    flags UInt8,
    `schema` Int8,
    zero_threshold Float64,
    count Float64,
    sum Float64,
    zero_count Float64,
    positive_spans Array(Tuple(offset Int32, length UInt32)),
    positive_values Array(Float64),
    negative_spans Array(Tuple(offset Int32, length UInt32)),
    negative_values Array(Float64),
    custom_values Array(Float64),
    count_int UInt64 MATERIALIZED toUInt64(count),
    zero_count_int UInt64 MATERIALIZED toUInt64(zero_count),
    positive_values_int Array(UInt64) MATERIALIZED arrayMap(v -> toUInt64(v), positive_values),
    negative_values_int Array(UInt64) MATERIALIZED arrayMap(v -> toUInt64(v), negative_values)
) ENGINE = Memory;

-- The samples of 05031: e1@110 and e2@120 fall into the same bucket (105, 120], e3@130 (a counter reset)
-- into (120, 135], e4@140 into (135, 150]. They are inserted in the order e1, e3, e2, e4.
INSERT INTO hist_samples_out_of_order VALUES
    (110, 0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], []),
    (130, 0, 0, 0., 2., 5., 0., [(0, 1)], [2.], [], [], []),
    (120, 0, 0, 0., 8., 21., 0., [(0, 2)], [2., 6.], [], [], []),
    (140, 0, 0, 0., 5., 11., 0., [(0, 2)], [2., 3.], [], [], []);

SELECT '-- add path: e2 arrives after e3, so the slice of the bucket (105, 120] is copied forward';
SELECT timeSeriesHistogramIncreaseToGrid(90, 210, 15, 45)(timestamp, tuple(flags, `schema`, zero_threshold, count, sum, zero_count, positive_spans, positive_values, negative_spans, negative_values, custom_values, count_int, zero_count_int, positive_values_int, negative_values_int))
FROM hist_samples_out_of_order
SETTINGS max_threads = 1;

SELECT '-- merge path: the states {e1, e3} and {e2, e3, e4} both have samples in the buckets (105, 120] and (120, 135]';
SELECT '-- (the duplicate e3 resolves to one sample)';
SELECT timeSeriesHistogramIncreaseToGridMerge(90, 210, 15, 45)(state)
FROM
(
    SELECT timeSeriesHistogramIncreaseToGridState(90, 210, 15, 45)(timestamp, tuple(flags, `schema`, zero_threshold, count, sum, zero_count, positive_spans, positive_values, negative_spans, negative_values, custom_values, count_int, zero_count_int, positive_values_int, negative_values_int)) AS state
    FROM hist_samples_out_of_order WHERE timestamp IN (110, 130)
    UNION ALL
    SELECT timeSeriesHistogramIncreaseToGridState(90, 210, 15, 45)(timestamp, tuple(flags, `schema`, zero_threshold, count, sum, zero_count, positive_spans, positive_values, negative_spans, negative_values, custom_values, count_int, zero_count_int, positive_values_int, negative_values_int)) AS state
    FROM hist_samples_out_of_order WHERE timestamp IN (120, 130, 140)
);

DROP TABLE hist_samples_out_of_order;
