-- Tags: no-fasttest
-- no-fasttest: ANTLR4 support is disabled in the fast-test build, and the PromQL grammar needs it.

-- Test: PromQL `rate`, `increase`, `delta`, `irate`, `idelta` over native-histogram series, end to end
-- through the PromQL converter: the histogram arms route to the
-- `timeSeriesHistogram{Rate,Increase,Delta,InstantRate,InstantDelta}ToGrid` aggregates (see
-- 05031_timeseries_histogram_rate_aggregates for the aggregate-level math), the float arm keeps
-- working, and mixed float+histogram windows drop the element (upstream
-- NewMixedFloatsHistogramsWarning; for irate/idelta only the two newest samples decide).
--
-- NOTE: this reference file was hand-computed through the upstream algorithm (bit-for-bit verified
-- against a Python recomputation of it and a run of the pinned upstream Go implementation).

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_nh_rate;
CREATE TABLE ts_nh_rate ENGINE = TimeSeries SETTINGS store_native_histograms = 1;

-- nh_counter{job='counter'}: exponential schema 0; e1@110 (count 4, sum 10), e2@120 (count 8, sum 21),
-- e3@130 (RESET: count 2, sum 5), e4@140 (count 5, sum 11).
INSERT INTO ts_nh_rate (metric_name, tags, histograms) VALUES
    ('nh_counter', map('job', 'counter'), [
        (toDateTime64(110, 3), 0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], []),
        (toDateTime64(120, 3), 0, 0, 0., 8., 21., 0., [(0, 2)], [2., 6.], [], [], []),
        (toDateTime64(130, 3), 0, 0, 0., 2., 5., 0., [(0, 1)], [2.], [], [], []),
        (toDateTime64(140, 3), 0, 0, 0., 5., 11., 0., [(0, 2)], [2., 3.], [], [], [])]);

-- nh_gauge{job='gauge'}: flags 6 = gauge counter-reset hint; g1@120 (count 5, sum 11), g2@140 (count 3, sum 6).
INSERT INTO ts_nh_rate (metric_name, tags, histograms) VALUES
    ('nh_gauge', map('job', 'gauge'), [
        (toDateTime64(120, 3), 6, 0, 0., 5., 11., 0., [(0, 2)], [2., 3.], [], [], []),
        (toDateTime64(140, 3), 6, 0, 0., 3., 6., 0., [(0, 2)], [1., 2.], [], [], [])]);

-- nh_custom{job='custom'}: custom buckets [1,2,4]; c1@110 (count 4), c2@120 (count 8), c2b@140 (count 12).
INSERT INTO ts_nh_rate (metric_name, tags, histograms) VALUES
    ('nh_custom', map('job', 'custom'), [
        (toDateTime64(110, 3), 0, -53, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [1., 2., 4.]),
        (toDateTime64(120, 3), 0, -53, 0., 8., 21., 0., [(0, 3)], [0., 2., 6.], [], [], [1., 2., 4.]),
        (toDateTime64(140, 3), 0, -53, 0., 12., 30., 0., [(0, 3)], [1., 3., 8.], [], [], [1., 2., 4.])]);

-- pure_float{job='float'}: 4@110, 8@120, 14@140.
INSERT INTO ts_nh_rate (metric_name, tags, time_series) VALUES
    ('pure_float', map('job', 'float'), [(toDateTime64(110, 3), 4), (toDateTime64(120, 3), 8), (toDateTime64(140, 3), 14)]);

-- mixed{job='mixed'}: a float sample at 108 and histograms at 130/140 (e3/e4).
INSERT INTO ts_nh_rate (metric_name, tags, time_series) VALUES
    ('mixed', map('job', 'mixed'), [(toDateTime64(108, 3), 100)]);
INSERT INTO ts_nh_rate (metric_name, tags, histograms) VALUES
    ('mixed', map('job', 'mixed'), [
        (toDateTime64(130, 3), 0, 0, 0., 2., 5., 0., [(0, 1)], [2.], [], [], []),
        (toDateTime64(140, 3), 0, 0, 0., 5., 11., 0., [(0, 2)], [2., 3.], [], [], [])]);

-- mixed2{job='mixed2'}: a histogram at 110 (e1) and a float at 140.
INSERT INTO ts_nh_rate (metric_name, tags, time_series) VALUES
    ('mixed2', map('job', 'mixed2'), [(toDateTime64(140, 3), 50)]);
INSERT INTO ts_nh_rate (metric_name, tags, histograms) VALUES
    ('mixed2', map('job', 'mixed2'), [(toDateTime64(110, 3), 0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [])]);

SELECT '-- rate over the counter series (a reset between the 2nd and 3rd samples): the increase is';
SELECT '-- e4 - nulled(e2) = e4 (the 1st sample is nulled because e3 resets against e2), extrapolated';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_rate', 'rate(nh_counter[45s])', 150);

SELECT '-- increase over the same series';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_rate', 'increase(nh_counter[45s])', 150);

SELECT '-- irate over the same series: the two most recent histograms (e3@130, e4@140)';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_rate', 'irate(nh_counter[45s])', 150);

SELECT '-- delta over the gauge series (counts decrease: no reset handling)';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_rate', 'delta(nh_gauge[45s])', 150);

SELECT '-- idelta over the gauge series: the two most recent samples, no extrapolation';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_rate', 'idelta(nh_gauge[45s])', 150);

SELECT '-- increase over the custom-bucket series';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_rate', 'increase(nh_custom[45s])', 150);

SELECT '-- rate over the pure-float series: the float arm (the value 1/3)';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_rate', 'rate(pure_float[45s])', 150);

SELECT '-- rate over the mixed series: float@108 and histograms@130,140 in one window -> dropped';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_rate', 'rate(mixed[45s])', 150);

SELECT '-- irate over the mixed series: the two NEWEST samples are both histograms -> not mixed';
SELECT '-- (upstream `instantValue` only considers the two newest samples of either kind)';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_rate', 'irate(mixed[45s])', 150);

SELECT '-- irate over the other mixed series: the two newest samples are a histogram and a float';
SELECT '-- -> mixed, dropped';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_rate', 'irate(mixed2[45s])', 150);

SELECT '-- rate over the other mixed series: a mixed window -> dropped';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_rate', 'rate(mixed2[45s])', 150);

SELECT '-- rate over a subquery (the inner grid resolves to e2@120, e3@135, e4@150 with the inner';
SELECT '-- step timestamps; the reset between e2 and e3 nulls e2; extrapolation over grid-time samples)';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_rate', 'rate(nh_counter[60s:15s])', 150);

SELECT '-- range query over the counter series: rate at every step; steps whose window holds fewer';
SELECT '-- than two samples emit nothing, and the float arm stays empty throughout';
SELECT tags, time_series, histogram_series FROM prometheusQueryRange('ts_nh_rate', 'rate(nh_counter[45s])', 100, 200, 10);

SELECT '-- range query over the mixed series: steps whose window mixes float and histogram samples';
SELECT '-- are dropped from BOTH arms (upstream NewMixedFloatsHistogramsWarning)';
SELECT tags, time_series, histogram_series FROM prometheusQueryRange('ts_nh_rate', 'rate(mixed[45s])', 100, 200, 10);

DROP TABLE ts_nh_rate;
