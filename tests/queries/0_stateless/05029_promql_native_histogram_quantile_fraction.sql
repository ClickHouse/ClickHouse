-- Tags: no-fasttest
-- no-fasttest: ANTLR4 support is disabled in the fast-test build, and the PromQL grammar needs it.

-- Test: PromQL `histogram_quantile` (native branch + the pre-existing classic branch, UNION ALL)
-- and `histogram_fraction` (native only), end to end through the PromQL converter:
-- applyHistogramQuantile maps the native branch to `timeSeriesHistogramQuantile` per time step,
-- applyHistogramFraction maps to `timeSeriesHistogramFraction` (see the scalar-level test
-- 05028_timeseries_histogram_quantile_fraction for the algorithm math).
--
-- NOTE: this reference file was verified bit-for-bit against a standalone C++ run of the exact
-- operation sequence plus a Python recomputation, and cross-checked against the upstream Go
-- implementation.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_nh_qf;
CREATE TABLE ts_nh_qf ENGINE = TimeSeries SETTINGS store_native_histograms = 1;

-- Native-histogram series nh{job='e2e'}: custom buckets [1,2,4] with counts [0,2,6]
-- (buckets [-Inf,1]x0, [1,2]x2, [2,4]x6), count 8, sum 21.
INSERT INTO ts_nh_qf (metric_name, tags, histograms) VALUES
    ('nh', map('job', 'e2e'), [(toDateTime64(110, 3), 0, -53, 0., 8., 21., 0., [(0, 3)], [0., 2., 6.], [], [], [1., 2., 4.])]);

-- Classic-bucket series cb_bucket{job='classic', le='0.5'/'1'/'+Inf'} with cumulative counts 1/3/4:
-- the phi=0.5 quantile is rank 2 in the le=1 bucket -> 0.5 + (1-0.5)*(2-1)/(3-1) = 0.75.
INSERT INTO ts_nh_qf (metric_name, tags, time_series) VALUES
    ('cb_bucket', map('job', 'classic', 'le', '0.5'), [(toDateTime64(110, 3), 1)]),
    ('cb_bucket', map('job', 'classic', 'le', '1'), [(toDateTime64(110, 3), 3)]),
    ('cb_bucket', map('job', 'classic', 'le', '+Inf'), [(toDateTime64(110, 3), 4)]);

-- A pure-float series: native-histogram functions skip it.
INSERT INTO ts_nh_qf (metric_name, tags, time_series) VALUES
    ('f', map('job', 'float'), [(toDateTime64(110, 3), 42)]);

SELECT '-- histogram_quantile over a native histogram: q=0.5 lands on [2,4], fraction 1/3, linear -> 2 + 2/3';
SELECT tags, timestamp, value FROM prometheusQuery('ts_nh_qf', 'histogram_quantile(0.5, nh)', 120);

SELECT '-- histogram_quantile over classic buckets: the classic branch (UNION ALL with the empty native branch)';
SELECT tags, timestamp, value FROM prometheusQuery('ts_nh_qf', 'histogram_quantile(0.5, cb_bucket)', 120);

SELECT '-- histogram_quantile over both: UNION ALL of the classic and native branches';
SELECT tags, timestamp, value FROM prometheusQuery('ts_nh_qf', 'histogram_quantile(0.5, {__name__=~"nh|cb_bucket"})', 120) ORDER BY tags;

SELECT '-- out-of-range phi on a native histogram: phi 2 -> +Inf (the scalar function carries the semantics)';
SELECT tags, timestamp, value FROM prometheusQuery('ts_nh_qf', 'histogram_quantile(2, nh)', 120);

SELECT '-- histogram_quantile over a pure-float series: no histogram samples, the series is skipped';
SELECT tags, timestamp, value FROM prometheusQuery('ts_nh_qf', 'histogram_quantile(0.5, f)', 120);

SELECT '-- histogram_fraction over a native histogram: linear ranks 1 and 5 in [1,2] and [2,4] -> (5-1)/8 = 0.5';
SELECT tags, timestamp, value FROM prometheusQuery('ts_nh_qf', 'histogram_fraction(1.5, 3, nh)', 120);

SELECT '-- histogram_fraction over classic buckets: not supported, empty result';
SELECT tags, timestamp, value FROM prometheusQuery('ts_nh_qf', 'histogram_fraction(0, 1, cb_bucket)', 120);

SELECT '-- histogram_fraction over a pure-float series: empty result';
SELECT tags, timestamp, value FROM prometheusQuery('ts_nh_qf', 'histogram_fraction(0, 1, f)', 120);

SELECT '-- range query over the native histogram: the quantile is emitted at every step seeing the sample';
SELECT tags, time_series FROM prometheusQueryRange('ts_nh_qf', 'histogram_quantile(0.5, nh)', 100, 120, 10);

DROP TABLE ts_nh_qf;
