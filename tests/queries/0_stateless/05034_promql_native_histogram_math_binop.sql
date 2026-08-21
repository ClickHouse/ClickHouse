-- Tags: no-fasttest
-- no-fasttest: ANTLR4 support is disabled in the fast-test build, and the PromQL grammar needs it.

-- Test: PromQL arithmetic binary operators over native-histogram series, end to end through the
-- PromQL converter, mirroring `vectorElemBinop` in Prometheus promql/engine.go.
--
-- NOTE: this reference file was hand-computed through the upstream algorithm (bit-for-bit verified
-- against a Python recomputation of it; see 05033_timeseries_histogram_math_operators).

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_nh_math;
CREATE TABLE ts_nh_math ENGINE = TimeSeries SETTINGS store_native_histograms = 1;

-- All series that should match each other carry the same tags {job='x'} (the metric name is not
-- part of the join key). e1 @100: exponential schema 0, count 4, sum 10, buckets (0.5,1]x1, (1,2]x3.
INSERT INTO ts_nh_math (metric_name, tags, histograms) VALUES
    ('nh_e1', map('job', 'x'), [(toDateTime64(100, 3), 0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [])]);
-- e2 @100: count 8, sum 21, buckets x2, x6.
INSERT INTO ts_nh_math (metric_name, tags, histograms) VALUES
    ('nh_e2', map('job', 'x'), [(toDateTime64(100, 3), 0, 0, 0., 8., 21., 0., [(0, 2)], [2., 6.], [], [], [])]);
-- s1 @100: schema 1, count 8, sum 21, buckets (0.71,1]x1, (1,1.41]x1, (1.41,2]x6.
INSERT INTO ts_nh_math (metric_name, tags, histograms) VALUES
    ('nh_s1', map('job', 'x'), [(toDateTime64(100, 3), 0, 1, 0., 8., 21., 0., [(0, 3)], [1., 1., 6.], [], [], [])]);
-- c1 @100: custom bounds [1,2,4], count 4, sum 10, buckets (-Inf,1]x1, (1,2]x3.
INSERT INTO ts_nh_math (metric_name, tags, histograms) VALUES
    ('nh_c1', map('job', 'x'), [(toDateTime64(100, 3), 0, -53, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [1., 2., 4.])]);
-- c3 @100: custom bounds [2,4,8], count 4, sum 14, buckets (-Inf,2]x5, (2,4]x7.
INSERT INTO ts_nh_math (metric_name, tags, histograms) VALUES
    ('nh_c3', map('job', 'x'), [(toDateTime64(100, 3), 0, -53, 0., 4., 14., 0., [(0, 2)], [5., 7.], [], [], [2., 4., 8.])]);
-- f1 @100: float 5.
INSERT INTO ts_nh_math (metric_name, tags, time_series) VALUES
    ('nh_f1', map('job', 'x'), [(toDateTime64(100, 3), 5)]);
-- mx1 {job='mx'}: float 100 @100, histogram e1 @110; mx2 {job='mx'}: histogram e2 @100.
INSERT INTO ts_nh_math (metric_name, tags, time_series) VALUES
    ('mx1', map('job', 'mx'), [(toDateTime64(100, 3), 100)]);
INSERT INTO ts_nh_math (metric_name, tags, histograms) VALUES
    ('mx1', map('job', 'mx'), [(toDateTime64(110, 3), 0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [])]);
INSERT INTO ts_nh_math (metric_name, tags, histograms) VALUES
    ('mx2', map('job', 'mx'), [(toDateTime64(100, 3), 0, 0, 0., 8., 21., 0., [(0, 2)], [2., 6.], [], [], [])]);

SELECT '-- histogram + histogram: e1 + e2 = (count 12, sum 31, buckets x3, x9)';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', 'nh_e1 + nh_e2', 105);

SELECT '-- histogram - histogram: e2 - e1 = (count 4, sum 11), result marked as gauge (flags 6)';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', 'nh_e2 - nh_e1', 105);

SELECT '-- scalar * histogram and histogram * scalar: e1 * 2 = (count 8, sum 20, buckets x2, x6)';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', 'nh_e1 * 2', 105);
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', '2 * nh_e1', 105);

SELECT '-- histogram / scalar: e1 / 2 = (count 2, sum 5, buckets x0.5, x1.5)';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', 'nh_e1 / 2', 105);

SELECT '-- schema mismatch: e1 + s1 (schema 1) -> s1 reduced to schema 0 (buckets x1, x7), then added';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', 'nh_e1 + nh_s1', 105);

SELECT '-- custom buckets with mismatched bounds: c1 [1,2,4] + c3 [2,4,8] -> bounds intersected to [2,4],';
SELECT '-- c1 maps to [4,0,0] and c3 stays [5,7,0]: (count 8, sum 24, buckets x9, x7 over (-Inf,2], (2,4])';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', 'nh_c1 + nh_c3', 105);

SELECT '-- the same pair subtracted: c3 - c1 = (count 0, sum 4, buckets x1, x7), gauge (flags 6)';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', 'nh_c3 - nh_c1', 105);

SELECT '-- exp + custom: schema-incompatible -> the sample is dropped (empty result)';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', 'nh_e1 + nh_c1', 105);

SELECT '-- disallowed: histogram * histogram -> dropped';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', 'nh_e1 * nh_e2', 105);

SELECT '-- disallowed: histogram / histogram -> dropped';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', 'nh_e1 / nh_e2', 105);

SELECT '-- disallowed: histogram % histogram, histogram ^ histogram, atan2 -> dropped';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', 'nh_e1 % nh_e2', 105);
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', 'nh_e1 ^ nh_e2', 105);
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', 'nh_e1 atan2 nh_e2', 105);

SELECT '-- disallowed: scalar / histogram and scalar + histogram -> dropped';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', '2 / nh_e1', 105);
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', '2 + nh_e1', 105);

SELECT '-- float series f1 = 5 combined with histogram e1 per step:';
SELECT '-- f1 + 1: the pure-float arm still works (5 + 1 = 6)';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', 'nh_f1 + 1', 105);

SELECT '-- f1 + e1: float + histogram -> dropped';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', 'nh_f1 + nh_e1', 105);

SELECT '-- f1 * e1 and e1 * f1: float * histogram is allowed: e1 * 5 = (count 20, sum 50, buckets x5, x15)';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', 'nh_f1 * nh_e1', 105);
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', 'nh_e1 * nh_f1', 105);

SELECT '-- e1 / f1: histogram / float is allowed: (count 0.8, sum 2, buckets x0.2, x0.6)';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', 'nh_e1 / nh_f1', 105);

SELECT '-- f1 / e1: float / histogram is NOT allowed -> dropped';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', 'nh_f1 / nh_e1', 105);

SELECT '-- mixed-kind series, per-step resolution: mx1 (float@100, histogram e1@110) + mx2 (histogram e2@100):';
SELECT '-- at 105 mx1 resolves to a float -> dropped; at 115 to the histogram -> e1 + e2';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', 'mx1 + mx2', 105);
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', 'mx1 + mx2', 115);

SELECT '-- one-to-many matching: g1 {job=grp, inst=a} (histogram e1) * on(job) group_left() g2 {job=grp} (float 2)';
SELECT '-- -> e1 * 2 with the tags of the "many" side';
INSERT INTO ts_nh_math (metric_name, tags, histograms) VALUES
    ('nh_g1', map('job', 'grp', 'inst', 'a'), [(toDateTime64(100, 3), 0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [])]);
INSERT INTO ts_nh_math (metric_name, tags, time_series) VALUES
    ('nh_g2', map('job', 'grp'), [(toDateTime64(100, 3), 2)]);
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', 'nh_g1 * on(job) group_left() nh_g2', 105);

SELECT '-- non-matching series are dropped: e1 {job=x} + g2 {job=grp} -> no match -> empty';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_math', 'nh_e1 + nh_g2', 105);

SELECT '-- range query: e1 + e2 at every step';
SELECT tags, time_series, histogram_series FROM prometheusQueryRange('ts_nh_math', 'nh_e1 + nh_e2', 100, 130, 15);

SELECT '-- range query of the disallowed `histogram * histogram`: both arms stay empty';
SELECT tags, time_series, histogram_series FROM prometheusQueryRange('ts_nh_math', 'nh_e1 * nh_e2', 100, 130, 15);

SELECT '-- range query of the mixed-kind series: at 100/115/130 mx1 resolves to float/histogram/histogram';
SELECT '-- -> drop / e1+e2 / e1+e2';
SELECT tags, time_series, histogram_series FROM prometheusQueryRange('ts_nh_math', 'mx1 + mx2', 100, 130, 15);

DROP TABLE ts_nh_math;
