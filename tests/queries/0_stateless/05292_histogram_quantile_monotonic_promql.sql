-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- histogram_quantile forces monotonic buckets and gives NaN for a histogram without observations, like Prometheus.

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

SELECT value FROM prometheusQuery(ts, 'histogram_quantile(0.85, hbroken_bucket)', 1700000000);
SELECT value FROM prometheusQuery(ts, 'histogram_quantile(0.01, nonmonotonic_bucket)', 1700000000);
SELECT value FROM prometheusQuery(ts, 'histogram_quantile(0.5, nonmonotonic_bucket)', 1700000000);
SELECT value FROM prometheusQuery(ts, 'histogram_quantile(0.99, nonmonotonic_bucket)', 1700000000);
SELECT value FROM prometheusQuery(ts, 'histogram_quantile(1, empty_bucket)', 1700000000);

DROP TABLE ts;
