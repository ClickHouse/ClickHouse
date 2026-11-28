-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- The PromQL `quantile` aggregation keeps NaN samples and orders them before every real value, like Prometheus,
-- and interpolates between two samples as `lower * (1 - weight) + upper * weight`, like Prometheus.

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
    ('series', map('point', 'c'), [(toDateTime64(110, 3), 5), (toDateTime64(120, 3), nan)]),
    ('nan_inf', map('point', 'a'), [(toDateTime64(60, 3), nan)]),
    ('nan_inf', map('point', 'b'), [(toDateTime64(60, 3), -inf)]),
    ('nan_inf', map('point', 'c'), [(toDateTime64(60, 3), 0)]),
    ('nan_inf', map('point', 'd'), [(toDateTime64(60, 3), 1)]),
    ('neg_inf', map('point', 'a'), [(toDateTime64(60, 3), -inf)]),
    ('neg_inf', map('point', 'b'), [(toDateTime64(60, 3), 0)]);

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

-- A NaN shifts the rank also next to a real -Inf: {NaN, -Inf, 0, 1} gives 0.4 as in Prometheus, without the NaN it would be 0.6.
SELECT '-- quantile(0.8, nan_inf)';
SELECT tags, value FROM prometheusQuery('prometheus', 'quantile(0.8, nan_inf)', 60) ORDER BY tags;

-- Between a real -Inf and 0 the result is -Inf, as in Prometheus, also when a NaN shifts the rank there.
SELECT '-- quantile(0.6, nan_inf)';
SELECT tags, value FROM prometheusQuery('prometheus', 'quantile(0.6, nan_inf)', 60) ORDER BY tags;
SELECT '-- quantile(0.5, neg_inf)';
SELECT tags, value FROM prometheusQuery('prometheus', 'quantile(0.5, neg_inf)', 60) ORDER BY tags;

SELECT '-- quantile(0.5, series), range';
SELECT * FROM prometheusQueryRange('prometheus', 'quantile(0.5, series)', 100, 120, 10) ORDER BY tags;
SELECT '-- quantile by (point) (0.5, series), range';
SELECT * FROM prometheusQueryRange('prometheus', 'quantile by (point) (0.5, series)', 100, 120, 10) ORDER BY tags;

DROP TABLE prometheus;
