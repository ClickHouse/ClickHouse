-- Tags: no-fasttest
-- no-fasttest: ANTLR4 support is disabled in the fast-test build, and the PromQL grammar needs it.

-- Test: PromQL `histogram_avg` / `histogram_stddev` / `histogram_stdvar` over a native-histogram
-- series, end to end through the PromQL converter (applyNativeHistogramFunction wraps
-- `timeSeriesHistogramAvg`/`timeSeriesHistogramStddev`/`timeSeriesHistogramStdvar` per time step).
--
-- NOTE: this reference file was verified bit-for-bit against a standalone C++ run of the exact
-- operation sequence and a Python recomputation of the upstream algorithm.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_nh_stats;
CREATE TABLE ts_nh_stats ENGINE = TimeSeries SETTINGS store_native_histograms = 1;

-- Custom-bucket histogram (schema -53) with bounds [1,2,4]: the [-Inf,1) bucket is empty (skipped),
-- [1,2) has count 2 (representative value (1+2)/2 = 1.5), [2,4) has count 6 (value (2+4)/2 = 3).
-- count 8, sum 21 -> avg = 21/8 = 2.625;
-- variance = (2*(1.5-2.625)^2 + 6*(3-2.625)^2)/8 = (2*1.265625 + 6*0.140625)/8 = 3.375/8 = 0.421875;
-- stddev = sqrt(0.421875).
INSERT INTO ts_nh_stats (metric_name, tags, histograms) VALUES
    ('h', map('job', 'e2e'), [(toDateTime64(110, 3), 0, -53, 0., 8., 21., 0., [(0, 3)], [0., 2., 6.], [], [], [1., 2., 4.])]);

SELECT '-- histogram_avg';
SELECT tags, timestamp, value FROM prometheusQuery('ts_nh_stats', 'histogram_avg(h)', 120);

SELECT '-- histogram_stddev';
SELECT tags, timestamp, value FROM prometheusQuery('ts_nh_stats', 'histogram_stddev(h)', 120);

SELECT '-- histogram_stdvar';
SELECT tags, timestamp, value FROM prometheusQuery('ts_nh_stats', 'histogram_stdvar(h)', 120);

DROP TABLE ts_nh_stats;
