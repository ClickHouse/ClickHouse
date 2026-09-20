-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Tests streaming aggregations over TimeSeries storage.
DROP TABLE IF EXISTS prometheus_aggr;

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;

CREATE TABLE prometheus_aggr ENGINE = TimeSeries;

-- Insert multi-series data with multiple hosts and data centers.
INSERT INTO prometheus_aggr (metric_name, tags, time_series) VALUES
    ('m', map('host', 'h1', 'dc', 'a'), [(toDateTime64(100, 3), 10), (toDateTime64(110, 3), 20), (toDateTime64(120, 3), 30), (toDateTime64(130, 3), 40)]),
    ('m', map('host', 'h2', 'dc', 'a'), [(toDateTime64(100, 3), 5), (toDateTime64(110, 3), 15), (toDateTime64(120, 3), 25), (toDateTime64(130, 3), 35)]),
    ('m', map('host', 'h3', 'dc', 'b'), [(toDateTime64(100, 3), 2), (toDateTime64(110, 3), 4), (toDateTime64(120, 3), 6), (toDateTime64(130, 3), 8)]),
    ('m', map('host', 'h4', 'dc', 'b'), [(toDateTime64(100, 3), 1), (toDateTime64(110, 3), 3), (toDateTime64(120, 3), 5), (toDateTime64(130, 3), 7)]);

SELECT '-- sum by (dc) (m), instant';
SELECT * FROM prometheusQuery('prometheus_aggr', 'sum by (dc) (m)', 130) ORDER BY tags;

SELECT '-- avg by (dc) (m), instant';
SELECT * FROM prometheusQuery('prometheus_aggr', 'avg by (dc) (m)', 130) ORDER BY tags;

SELECT '-- min by (dc) (m), instant';
SELECT * FROM prometheusQuery('prometheus_aggr', 'min by (dc) (m)', 130) ORDER BY tags;

SELECT '-- max by (dc) (m), instant';
SELECT * FROM prometheusQuery('prometheus_aggr', 'max by (dc) (m)', 130) ORDER BY tags;

SELECT '-- count by (dc) (m), instant';
SELECT * FROM prometheusQuery('prometheus_aggr', 'count by (dc) (m)', 130) ORDER BY tags;

SELECT '-- sum without (host) (m), instant';
SELECT * FROM prometheusQuery('prometheus_aggr', 'sum without (host) (m)', 130) ORDER BY tags;

SELECT '-- sum by (dc) (rate(m[20s])), range';
SELECT * FROM prometheusQueryRange('prometheus_aggr', 'sum by (dc) (rate(m[20s]))', 110, 130, 10) ORDER BY tags;

DROP TABLE prometheus_aggr;
