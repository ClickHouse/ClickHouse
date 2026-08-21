-- Tags: no-fasttest
-- no-fasttest: ANTLR4 support is disabled in the fast-test build, and the PromQL grammar needs it.

-- Test: instant PromQL evaluation over mixed-type series (float and native-histogram samples in one
-- series) returns the newest sample of EITHER type per series (StoreMethod::HISTOGRAM_GRID resolves
-- the precedence via the `sample_kinds` grid column; see applyFunctionOverRange / finalizeSQL).
-- Covered: a pure-float series, a pure-histogram series, a mixed series with a newer float,
-- a mixed series with a newer histogram, and a stale-marker histogram sample (the HTTP JSON writer
-- skips it; at the SQL level it is still returned, flagged).
--
-- NOTE: this reference file was hand-computed through the verified converter semantics.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_nh;
CREATE TABLE ts_nh ENGINE = TimeSeries SETTINGS store_native_histograms = 1;

INSERT INTO ts_nh (metric_name, tags, time_series) VALUES
    ('pure_float', map('job', 'a'), [(toDateTime64(100, 3), 1.5), (toDateTime64(110, 3), 2.5)]),
    ('mixed_float_newer', map('job', 'c'), [(toDateTime64(110, 3), 42)]),
    ('mixed_hist_newer', map('job', 'd'), [(toDateTime64(100, 3), 3.25)]);

-- The `histograms` outer column carries one tuple per sample:
-- (timestamp, flags, schema, zero_threshold, count, sum, zero_count, positive_spans, positive_values,
--  negative_spans, negative_values, custom_values). flags = 16 is the stale-marker bit.
INSERT INTO ts_nh (metric_name, tags, histograms) VALUES
    ('pure_hist', map('job', 'b'), [(toDateTime64(110, 3), 0, 0, 0.001, 10, 25.5, 2, [(0, 2), (1, 1)], [3, 2, 3], [], [], [])]),
    ('mixed_float_newer', map('job', 'c'), [(toDateTime64(100, 3), 0, 0, 0.001, 5, 7.5, 1, [(0, 1)], [4], [], [], [])]),
    ('mixed_hist_newer', map('job', 'd'), [(toDateTime64(110, 3), 0, 0, 0.001, 7, 11.5, 0, [(0, 2)], [4, 3], [], [], [])]),
    ('stale_hist', map('job', 'e'), [(toDateTime64(110, 3), 16, 0, 0.001, 9, 9, 0, [(0, 1)], [9], [], [], [])]);

SELECT '-- pure-float series: the float sample wins, histogram is NULL';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh', 'pure_float', 120);

SELECT '-- pure-histogram series: the histogram sample wins, value is NULL';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh', 'pure_hist', 120);

SELECT '-- mixed series, float newer (float@110 vs histogram@100): the float wins';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh', 'mixed_float_newer', 120);

SELECT '-- mixed series, histogram newer (float@100 vs histogram@110): the histogram wins';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh', 'mixed_hist_newer', 120);

SELECT '-- histogram_count over the pure-histogram series';
SELECT tags, timestamp, value FROM prometheusQuery('ts_nh', 'histogram_count(pure_hist)', 120);

SELECT '-- histogram_sum over the mixed series whose newest sample is a histogram';
SELECT tags, timestamp, value FROM prometheusQuery('ts_nh', 'histogram_sum(mixed_hist_newer)', 120);

SELECT '-- histogram_count over the mixed series whose newest sample is a float: the series is skipped';
SELECT tags, timestamp, value FROM prometheusQuery('ts_nh', 'histogram_count(mixed_float_newer)', 120);

SELECT '-- histogram_count over a pure-float series: no histogram samples, the series is skipped';
SELECT tags, timestamp, value FROM prometheusQuery('ts_nh', 'histogram_count(pure_float)', 120);

SELECT '-- no samples in the lookback window: empty result';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh', 'pure_float', 50);

SELECT '-- stale-marker histogram: still returned at the SQL level with the stale flag set';
SELECT '-- (the HTTP JSON writer skips it, and a block emitting zero rows stays valid JSON - see PrometheusHTTPProtocolAPI)';
SELECT tags, timestamp, value, tupleElement(histogram, 'flags') AS histogram_flags
    FROM prometheusQuery('ts_nh', 'stale_hist', 120);

SELECT '-- range query over a mixed series: both arms are still emitted (SampleStream semantics)';
SELECT tags, time_series, histogram_series FROM prometheusQueryRange('ts_nh', 'mixed_hist_newer', 100, 120, 10);

DROP TABLE ts_nh;
