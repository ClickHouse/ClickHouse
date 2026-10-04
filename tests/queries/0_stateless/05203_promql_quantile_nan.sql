-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- The PromQL `quantile` aggregation keeps NaN samples and orders them before every real value, like Prometheus.

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS prometheus;
CREATE TABLE prometheus ENGINE = TimeSeries;

INSERT INTO prometheus (metric_name, tags, samples) VALUES
    ('data', map('test', 'two samples', 'point', 'a'), [(toDateTime64(60, 3), 0)]),
    ('data', map('test', 'two samples', 'point', 'b'), [(toDateTime64(60, 3), 1)]),
    ('data', map('test', 'three samples', 'point', 'a'), [(toDateTime64(60, 3), 0)]),
    ('data', map('test', 'three samples', 'point', 'b'), [(toDateTime64(60, 3), 1)]),
    ('data', map('test', 'three samples', 'point', 'c'), [(toDateTime64(60, 3), 2)]),
    ('data', map('test', 'NaN sample', 'point', 'a'), [(toDateTime64(60, 3), 0)]),
    ('data', map('test', 'NaN sample', 'point', 'b'), [(toDateTime64(60, 3), 1)]),
    ('data', map('test', 'NaN sample', 'point', 'c'), [(toDateTime64(60, 3), nan)]),
    ('data', map('test', 'only NaN', 'point', 'a'), [(toDateTime64(60, 3), nan)]),
    ('data', map('test', 'only NaN', 'point', 'b'), [(toDateTime64(60, 3), nan)]),
    ('data', map('test', 'two NaN', 'point', 'a'), [(toDateTime64(60, 3), nan)]),
    ('data', map('test', 'two NaN', 'point', 'b'), [(toDateTime64(60, 3), nan)]),
    ('data', map('test', 'two NaN', 'point', 'c'), [(toDateTime64(60, 3), 3)]),
    ('foo', map(), [(toDateTime64(60, 3), 0.8)]),
    ('series', map('point', 'a'), [(toDateTime64(100, 3), 1), (toDateTime64(110, 3), nan), (toDateTime64(120, 3), 3)]),
    ('series', map('point', 'b'), [(toDateTime64(100, 3), 2), (toDateTime64(110, 3), 2), (toDateTime64(120, 3), 2)]),
    ('series', map('point', 'c'), [(toDateTime64(110, 3), 5), (toDateTime64(120, 3), nan)]);

SELECT '-- quantile without(point)(0, data)';
SELECT tags, value FROM prometheusQuery('prometheus', 'quantile without(point)(0, data)', 60) ORDER BY tags;
SELECT '-- quantile without(point)(0.2, data)';
SELECT tags, value FROM prometheusQuery('prometheus', 'quantile without(point)(0.2, data)', 60) ORDER BY tags;
SELECT '-- quantile without(point)(0.5, data)';
SELECT tags, value FROM prometheusQuery('prometheus', 'quantile without(point)(0.5, data)', 60) ORDER BY tags;
SELECT '-- quantile without(point)(0.8, data)';
SELECT tags, value FROM prometheusQuery('prometheus', 'quantile without(point)(0.8, data)', 60) ORDER BY tags;
SELECT '-- quantile without(point)(1, data)';
SELECT tags, value FROM prometheusQuery('prometheus', 'quantile without(point)(1, data)', 60) ORDER BY tags;

SELECT '-- quantile without(point)(scalar(foo), data)';
SELECT tags, value FROM prometheusQuery('prometheus', 'quantile without(point)(scalar(foo), data)', 60) ORDER BY tags;
SELECT '-- quantile without(point)(scalar(foo) - 1, data)';
SELECT tags, value FROM prometheusQuery('prometheus', 'quantile without(point)(scalar(foo) - 1, data)', 60) ORDER BY tags;

SELECT '-- quantile(0.5, series), range';
SELECT * FROM prometheusQueryRange('prometheus', 'quantile(0.5, series)', 100, 120, 10) ORDER BY tags;
SELECT '-- quantile by (point) (0.5, series), range';
SELECT * FROM prometheusQueryRange('prometheus', 'quantile by (point) (0.5, series)', 100, 120, 10) ORDER BY tags;

DROP TABLE prometheus;
