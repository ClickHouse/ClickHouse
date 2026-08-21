-- Tags: no-fasttest
-- no-fasttest: ANTLR4 support is disabled in the fast-test build, and the PromQL grammar needs it.

-- Test: PromQL `sum`/`avg` aggregation operators over native-histogram series, end to end through
-- the PromQL converter, mirroring the parser.SUM/parser.AVG histogram branches of `aggregation` in
-- Prometheus promql/engine.go. Other aggregation operators keep ignoring histograms (the float arm).
--
-- NOTE: this reference file was hand-computed through the upstream algorithm (bit-for-bit verified
-- against a Python recomputation of it; see 05033_timeseries_histogram_math_operators for the
-- scalar/aggregate-level coverage of the same kernel paths).

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_nh_agg;
CREATE TABLE ts_nh_agg ENGINE = TimeSeries SETTINGS store_native_histograms = 1;

-- Group {job='exp'}: e1 (count 4, sum 10, buckets (0.5,1]x1, (1,2]x3) and e2 (count 8, sum 21, x2, x6), both @100.
INSERT INTO ts_nh_agg (metric_name, tags, histograms) VALUES
    ('nh_e1', map('job', 'exp'), [(toDateTime64(100, 3), 0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [])]),
    ('nh_e2', map('job', 'exp'), [(toDateTime64(100, 3), 0, 0, 0., 8., 21., 0., [(0, 2)], [2., 6.], [], [], [])]);
-- Group {job='custom'}: c1 (custom bounds [1,2,4], count 4, sum 10, buckets (-Inf,1]x1, (1,2]x3) and c2 (count 8, sum 21, x0/x2/x6), both @100.
INSERT INTO ts_nh_agg (metric_name, tags, histograms) VALUES
    ('nh_c1', map('job', 'custom'), [(toDateTime64(100, 3), 0, -53, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [1., 2., 4.])]),
    ('nh_c2', map('job', 'custom'), [(toDateTime64(100, 3), 0, -53, 0., 8., 21., 0., [(0, 3)], [0., 2., 6.], [], [], [1., 2., 4.])]);
-- Group {job='mixed'}: a float series (5 @100) and a histogram series (e1 @100).
INSERT INTO ts_nh_agg (metric_name, tags, time_series) VALUES
    ('mx_f', map('job', 'mixed'), [(toDateTime64(100, 3), 5)]);
INSERT INTO ts_nh_agg (metric_name, tags, histograms) VALUES
    ('mx_h', map('job', 'mixed'), [(toDateTime64(100, 3), 0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [])]);
-- Group {job='incompat'}: an exponential series (e1) and a custom series (c1).
INSERT INTO ts_nh_agg (metric_name, tags, histograms) VALUES
    ('ix_e', map('job', 'incompat'), [(toDateTime64(100, 3), 0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [])]),
    ('ix_c', map('job', 'incompat'), [(toDateTime64(100, 3), 0, -53, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [1., 2., 4.])]);
-- Group {job='float'}: two float series (5 and 7 @100).
INSERT INTO ts_nh_agg (metric_name, tags, time_series) VALUES
    ('fl_1', map('job', 'float'), [(toDateTime64(100, 3), 5)]),
    ('fl_2', map('job', 'float'), [(toDateTime64(100, 3), 7)]);
-- Group {job='kahan'}: big (count 1, sum 1e16) and two smalls (count 1, sum 1), all @100.
INSERT INTO ts_nh_agg (metric_name, tags, histograms) VALUES
    ('kbig', map('job', 'kahan'), [(toDateTime64(100, 3), 0, 0, 0., 1., 1e16, 0., [(0, 1)], [1e16], [], [], [])]),
    ('ksm1', map('job', 'kahan'), [(toDateTime64(100, 3), 0, 0, 0., 1., 1., 0., [(0, 1)], [1.], [], [], [])]),
    ('ksm2', map('job', 'kahan'), [(toDateTime64(100, 3), 0, 0, 0., 1., 1., 0., [(0, 1)], [1.], [], [], [])]);
-- Group {job='huge'}: two samples with count = sum = bucket = 1.5e308 @100.
INSERT INTO ts_nh_agg (metric_name, tags, histograms) VALUES
    ('huge1', map('job', 'huge'), [(toDateTime64(100, 3), 0, 0, 0., 1.5e308, 1.5e308, 0., [(0, 1)], [1.5e308], [], [], [])]),
    ('huge2', map('job', 'huge'), [(toDateTime64(100, 3), 0, 0, 0., 1.5e308, 1.5e308, 0., [(0, 1)], [1.5e308], [], [], [])]);

SELECT '-- sum by (job): exp -> (count 12, sum 31, buckets x3, x9), custom -> (count 12, sum 31,';
SELECT '-- buckets x1, x5, x6 over [1,2,4]), float -> 12; the mixed and incompatible groups drop entirely';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_agg', 'sum by (job) ({job=~"exp|custom|float|mixed|incompat"})', 105) ORDER BY tags;

SELECT '-- avg by (job) over the same groups: exp -> (count 6, sum 15.5, buckets x1.5, x4.5),';
SELECT '-- custom -> (count 6, sum 15.5, buckets x0.5, x2.5, x3), float -> 6; mixed/incompat dropped';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_agg', 'avg by (job) ({job=~"exp|custom|float|mixed|incompat"})', 105) ORDER BY tags;

SELECT '-- sum without grouping over the two exp series: one group (count 12, sum 31)';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_agg', 'sum({job="exp"})', 105);

SELECT '-- sum without (job) over the same: same single group';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_agg', 'sum without (job) ({job="exp"})', 105);

SELECT '-- avg over a single-histogram group: the sample itself';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_agg', 'avg(nh_e1)', 105);

SELECT '-- Kahan accumulation e2e: the naive sum loses the two ones (ulp(1e16) = 2), the Kahan sum';
SELECT '-- keeps them: sum = 1e16+2 = 10000000000000002, avg = 3333333333333334 (naive ...3333.5)';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_agg', 'sum({job="kahan"})', 105);
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_agg', 'avg({job="kahan"})', 105);

SELECT '-- overflow: the `sum` of the two 1.5e308 histograms overflows to +Inf (upstream keeps it),';
SELECT '-- while `avg` switches to the incremental mean and stays finite (1.5e308)';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_agg', 'sum({job="huge"})', 105);
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_agg', 'avg({job="huge"})', 105);

SELECT '-- a group whose only series has no sample in the lookback window produces nothing';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_agg', 'sum({job="exp"})', 500);

SELECT '-- the float arm of a mixed-kind series is masked per step: mx_h alone sums to the histogram';
SELECT '-- itself, while mx_f and mx_h together (one group mixing both kinds at the step) drop it';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_agg', 'sum(mx_h)', 105);
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_agg', 'sum({__name__=~"mx_f|mx_h"})', 105);

SELECT '-- range query: sum/avg by (job) over the exp group at every step';
SELECT tags, time_series, histogram_series FROM prometheusQueryRange('ts_nh_agg', 'sum by (job) ({job="exp"})', 100, 130, 15);
SELECT tags, time_series, histogram_series FROM prometheusQueryRange('ts_nh_agg', 'avg by (job) ({job="exp"})', 100, 130, 15);

SELECT '-- range query over the mixed group: every step drops (both arms empty)';
SELECT tags, time_series, histogram_series FROM prometheusQueryRange('ts_nh_agg', 'sum by (job) ({job="mixed"})', 100, 130, 15);

DROP TABLE ts_nh_agg;
