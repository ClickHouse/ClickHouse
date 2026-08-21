-- Test: the rate-family native-histogram aggregates `timeSeriesHistogram{Rate,Increase,Delta,InstantRate,InstantDelta}ToGrid`
-- over individual timestamp/histogram rows: reset-aware counter paths (rate/increase), gauge paths
-- (delta/idelta), exponential and custom buckets, schema reduction, zero-threshold reconciliation,
-- extrapolation boundaries, and the single-sample case. The math mirrors Prometheus' `histogramRate`
-- and `extrapolatedRate`/`instantValue` (see src/Functions/TimeSeries/TimeSeriesHistogramRate.h).
--
-- NOTE: this reference file was hand-computed through the upstream algorithm (bit-for-bit verified
-- against a Python recomputation of it and a run of the pinned upstream Go implementation).
--
-- The samples below (exponential schema 0; bucket idx 0 = (0.5,1], idx 1 = (1,2]) are named like
-- in the test comments: e1 = (count 4, sum 10), e2 = (count 8, sum 21), e3 = (count 2, sum 5,
-- a counter reset vs e2), e4 = (count 5, sum 11).

SET allow_experimental_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS hist_samples;
CREATE TABLE hist_samples
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
    custom_values Array(Float64)
) ENGINE = MergeTree ORDER BY timestamp;

INSERT INTO hist_samples VALUES
    (110, 0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], []),
    (120, 0, 0, 0., 8., 21., 0., [(0, 2)], [2., 6.], [], [], []),
    (130, 0, 0, 0., 2., 5., 0., [(0, 1)], [2.], [], [], []),
    (140, 0, 0, 0., 5., 11., 0., [(0, 2)], [2., 3.], [], [], []);

SELECT '-- counter with a mid-window reset, exponential schema: `timeSeriesHistogramIncreaseToGrid` over';
SELECT '-- samples e1@110 (count 4), e2@120 (count 8), e3@130 (RESET, count 2), e4@140 (count 5), window 45';
SELECT timeSeriesHistogramIncreaseToGrid(90, 210, 15, 45)(timestamp, tuple(flags, `schema`, zero_threshold, count, sum, zero_count, positive_spans, positive_values, negative_spans, negative_values, custom_values)) FROM hist_samples;

SELECT '-- the same series: `timeSeriesHistogramRateToGrid` divides the increase by the window (45)';
SELECT timeSeriesHistogramRateToGrid(90, 210, 15, 45)(timestamp, tuple(flags, `schema`, zero_threshold, count, sum, zero_count, positive_spans, positive_values, negative_spans, negative_values, custom_values)) FROM hist_samples;

SELECT '-- the same series with `DateTime64(3)` timestamps (millisecond grid): the increase is identical';
SELECT timeSeriesHistogramIncreaseToGrid(90, 210, 15, 45)(toDateTime64(timestamp, 3), tuple(flags, `schema`, zero_threshold, count, sum, zero_count, positive_spans, positive_values, negative_spans, negative_values, custom_values)) FROM hist_samples;

TRUNCATE TABLE hist_samples;
INSERT INTO hist_samples VALUES
    (120, 6, 0, 0., 5., 11., 0., [(0, 2)], [2., 3.], [], [], []),
    (140, 6, 0, 0., 3., 6., 0., [(0, 2)], [1., 2.], [], [], []);

SELECT '-- gauge series g1@120 (count 5), g2@140 (count 3): `timeSeriesHistogramDeltaToGrid` (no reset handling)';
SELECT timeSeriesHistogramDeltaToGrid(90, 210, 15, 45)(timestamp, tuple(flags, `schema`, zero_threshold, count, sum, zero_count, positive_spans, positive_values, negative_spans, negative_values, custom_values)) FROM hist_samples;

SELECT '-- the same series: `timeSeriesHistogramInstantDeltaToGrid` over the two most recent samples, no extrapolation';
SELECT timeSeriesHistogramInstantDeltaToGrid(90, 210, 15, 45)(timestamp, tuple(flags, `schema`, zero_threshold, count, sum, zero_count, positive_spans, positive_values, negative_spans, negative_values, custom_values)) FROM hist_samples;

TRUNCATE TABLE hist_samples;
INSERT INTO hist_samples VALUES
    (110, 0, -53, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [1., 2., 4.]),
    (120, 0, -53, 0., 8., 21., 0., [(0, 3)], [0., 2., 6.], [], [], [1., 2., 4.]),
    (130, 0, -53, 0., 2., 5., 0., [(0, 1)], [2.], [], [], [1., 2., 4.]),
    (140, 0, -53, 0., 8., 21., 0., [(0, 3)], [0., 2., 6.], [], [], [1., 2., 4.]);

SELECT '-- custom buckets [1,2,4], counter with a mid-window reset: `timeSeriesHistogramIncreaseToGrid` over';
SELECT '-- c1@110 (count 4), c2@120 (count 8), c4@130 (RESET, count 2), c2@140 (count 8), window 45';
SELECT timeSeriesHistogramIncreaseToGrid(90, 210, 15, 45)(timestamp, tuple(flags, `schema`, zero_threshold, count, sum, zero_count, positive_spans, positive_values, negative_spans, negative_values, custom_values)) FROM hist_samples;

TRUNCATE TABLE hist_samples;
INSERT INTO hist_samples VALUES
    (140, 0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], []),
    (150, 0, 0, 0., 8., 21., 0., [(0, 2)], [2., 6.], [], [], []);

SELECT '-- extrapolation boundary: samples e1@140, e2@150 with window 60 sit further than the';
SELECT '-- extrapolation threshold from the range edges at grid point 180 -> extrapolate by half';
SELECT '-- the average sample spacing on both sides; at 195 the window (135,195] holds both samples,';
SELECT '-- at 165 the window (105,165] holds both too, but durationToEnd == 30 >= threshold there';
SELECT timeSeriesHistogramRateToGrid(120, 210, 15, 60)(timestamp, tuple(flags, `schema`, zero_threshold, count, sum, zero_count, positive_spans, positive_values, negative_spans, negative_values, custom_values)) FROM hist_samples;

TRUNCATE TABLE hist_samples;
INSERT INTO hist_samples VALUES
    (140, 0, 0, 0., 5., 11., 0., [(0, 2)], [2., 3.], [], [], []);

SELECT '-- a single sample in the window can not produce a rate: `timeSeriesHistogramDeltaToGrid` over e4@140';
SELECT timeSeriesHistogramDeltaToGrid(90, 210, 15, 45)(timestamp, tuple(flags, `schema`, zero_threshold, count, sum, zero_count, positive_spans, positive_values, negative_spans, negative_values, custom_values)) FROM hist_samples;

TRUNCATE TABLE hist_samples;
INSERT INTO hist_samples VALUES
    (120, 0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], []),
    (140, 0, 0, 0., 8., 21., 0., [(0, 2)], [2., 6.], [], [], []);

SELECT '-- `timeSeriesHistogramInstantRateToGrid` over e1@120, e2@140 (no reset) and e2@120, e3@140 (reset:';
SELECT '-- the result is the newest sample scaled to per-second, mirroring upstream `instantValue`)';
SELECT timeSeriesHistogramInstantRateToGrid(150, 150, 15, 45)(timestamp, tuple(flags, `schema`, zero_threshold, count, sum, zero_count, positive_spans, positive_values, negative_spans, negative_values, custom_values)) FROM hist_samples;

TRUNCATE TABLE hist_samples;
INSERT INTO hist_samples VALUES
    (120, 0, 0, 0., 8., 21., 0., [(0, 2)], [2., 6.], [], [], []),
    (140, 0, 0, 0., 2., 5., 0., [(0, 1)], [2.], [], [], []);
SELECT timeSeriesHistogramInstantRateToGrid(150, 150, 15, 45)(timestamp, tuple(flags, `schema`, zero_threshold, count, sum, zero_count, positive_spans, positive_values, negative_spans, negative_values, custom_values)) FROM hist_samples;

TRUNCATE TABLE hist_samples;
INSERT INTO hist_samples VALUES
    (120, 0, 1, 0., 8., 21., 0., [(0, 4)], [1., 1., 2., 4.], [], [], []),
    (140, 0, 0, 0., 8., 21., 0., [(0, 2)], [2., 6.], [], [], []);

SELECT '-- schema reduction: `timeSeriesHistogramRateToGrid` over e5@120 (schema 1) and e2@140 (schema 0)';
SELECT timeSeriesHistogramRateToGrid(90, 210, 15, 45)(timestamp, tuple(flags, `schema`, zero_threshold, count, sum, zero_count, positive_spans, positive_values, negative_spans, negative_values, custom_values)) FROM hist_samples;

TRUNCATE TABLE hist_samples;
INSERT INTO hist_samples VALUES
    (120, 0, 0, 0.1, 9., 22., 1., [(0, 2)], [2., 6.], [], [], []),
    (140, 0, 0, 0., 8., 21., 0., [(0, 2)], [2., 6.], [], [], []);

SELECT '-- zero-threshold reconciliation: `timeSeriesHistogramRateToGrid` over ez@120 (zt 0.1) and e2@140 (zt 0)';
SELECT timeSeriesHistogramRateToGrid(90, 210, 15, 45)(timestamp, tuple(flags, `schema`, zero_threshold, count, sum, zero_count, positive_spans, positive_values, negative_spans, negative_values, custom_values)) FROM hist_samples;

DROP TABLE hist_samples;
