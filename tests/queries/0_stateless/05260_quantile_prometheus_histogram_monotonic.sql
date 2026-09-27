-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Like Prometheus, a decreasing cumulative bucket value is raised to the previous one, a tiny float
-- increase is ignored, and a histogram without observations gives NaN.

SELECT 'decreasing bucket is raised to the previous one';
SELECT quantilePrometheusHistogram(0.85)(le, count) FROM VALUES('le Float64, count Float64', (1, 10), (2, 8), (inf, 10));
SELECT quantilePrometheusHistogram(0.85)(le, count) FROM VALUES('le Float64, count UInt64', (1, 10), (2, 8), (inf, 10));
SELECT quantilesPrometheusHistogram(0.5, 0.85)(le, count) FROM VALUES('le Float64, count Float64', (1, 10), (2, 8), (inf, 10));

SELECT 'decreasing +Inf bucket';
SELECT quantilesPrometheusHistogram(0.01, 0.5, 0.99)(le, count)
FROM VALUES('le Float64, count Float64', (0.1, 20), (1, 10), (10, 50), (100, 40), (1000, 90), (inf, 80));

SELECT 'tiny float increase is ignored, an integer one is not';
SELECT quantilePrometheusHistogram(1)(le, count) FROM VALUES('le Float64, count Float64', (1, 100), (2, 100), (inf, 100.00000000001));
SELECT quantilePrometheusHistogram(1)(le, count) FROM VALUES('le Float64, count UInt64', (1, 10000000000000), (2, 10000000000000), (inf, 10000000000001));

SELECT 'no observations';
SELECT quantilePrometheusHistogram(1)(le, count) FROM VALUES('le Float64, count Float64', (0, 0), (1, 0), (2, 0), (inf, 0));
SELECT quantilesPrometheusHistogram(0, 1)(le, count) FROM VALUES('le Float64, count UInt64', (0, 0), (1, 0), (inf, 0));

SET enable_time_series_table = 1;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

INSERT INTO ts (metric_name, tags, samples) VALUES
    ('hbroken_bucket', map('le', '1'), [(toDateTime64(1700000000, 3), 10)]),
    ('hbroken_bucket', map('le', '2'), [(toDateTime64(1700000000, 3), 8)]),
    ('hbroken_bucket', map('le', '+Inf'), [(toDateTime64(1700000000, 3), 10)]),
    ('nonmonotonic_bucket', map('le', '0.1'), [(toDateTime64(1700000000, 3), 20)]),
    ('nonmonotonic_bucket', map('le', '1'), [(toDateTime64(1700000000, 3), 10)]),
    ('nonmonotonic_bucket', map('le', '10'), [(toDateTime64(1700000000, 3), 50)]),
    ('nonmonotonic_bucket', map('le', '100'), [(toDateTime64(1700000000, 3), 40)]),
    ('nonmonotonic_bucket', map('le', '1000'), [(toDateTime64(1700000000, 3), 90)]),
    ('nonmonotonic_bucket', map('le', '+Inf'), [(toDateTime64(1700000000, 3), 80)]),
    ('empty_bucket', map('le', '0'), [(toDateTime64(1700000000, 3), 0)]),
    ('empty_bucket', map('le', '1'), [(toDateTime64(1700000000, 3), 0)]),
    ('empty_bucket', map('le', '+Inf'), [(toDateTime64(1700000000, 3), 0)]);

SELECT 'PromQL';
SELECT value FROM prometheusQuery(ts, 'histogram_quantile(0.85, hbroken_bucket)', 1700000000);
SELECT value FROM prometheusQuery(ts, 'histogram_quantile(0.01, nonmonotonic_bucket)', 1700000000);
SELECT value FROM prometheusQuery(ts, 'histogram_quantile(0.5, nonmonotonic_bucket)', 1700000000);
SELECT value FROM prometheusQuery(ts, 'histogram_quantile(0.99, nonmonotonic_bucket)', 1700000000);
SELECT value FROM prometheusQuery(ts, 'histogram_quantile(1, empty_bucket)', 1700000000);

DROP TABLE ts;
