-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- An operator on two constant scalars gives a constant scalar, so functions requiring a constant parameter accept it.

SET enable_time_series_table = 1;
SET enable_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

INSERT INTO ts (metric_name, tags, samples) VALUES
    ('req_bucket', map('le', '0.1'), [(toDateTime64(100, 3), 1.0)]),
    ('req_bucket', map('le', '0.5'), [(toDateTime64(100, 3), 4.0)]),
    ('req_bucket', map('le', '1'), [(toDateTime64(100, 3), 7.0)]),
    ('req_bucket', map('le', '+Inf'), [(toDateTime64(100, 3), 8.0)]),
    ('m', map('i', '1'), [(toDateTime64(100, 3), 10.0)]),
    ('m', map('i', '2'), [(toDateTime64(100, 3), 20.0)]),
    ('m', map('i', '3'), [(toDateTime64(100, 3), 30.0)]),
    ('m', map('i', '4'), [(toDateTime64(100, 3), 40.0)]);

SELECT '-- histogram_quantile with a computed phi';
SELECT tags, value FROM prometheusQuery(ts, 'histogram_quantile(1./6., req_bucket)', 100);
SELECT tags, value FROM prometheusQuery(ts, 'histogram_quantile(0.16666666666666666, req_bucket)', 100);
SELECT tags, value FROM prometheusQuery(ts, 'histogram_quantile(1 - 0.75, req_bucket)', 100);
SELECT tags, value FROM prometheusQuery(ts, 'histogram_quantile(-(1 / 2), req_bucket)', 100);
SELECT tags, value FROM prometheusQuery(ts, 'histogram_quantile(1 + 1, req_bucket)', 100);
SELECT tags, value FROM prometheusQuery(ts, 'histogram_quantile(0 / 0, req_bucket)', 100);
SELECT tags, value FROM prometheusQuery(ts, 'histogram_quantile(1 > bool 0, req_bucket)', 100);

SELECT '-- other functions with a computed parameter';
SELECT tags, value FROM prometheusQuery(ts, 'quantile(1/4, m)', 100);
SELECT tags, value FROM prometheusQuery(ts, 'topk(3-1, m)', 100) ORDER BY value;
SELECT tags, value FROM prometheusQuery(ts, 'clamp(m, 5 + 10, 50 - 15)', 100) ORDER BY value;
SELECT tags, value FROM prometheusQuery(ts, 'm * (1 + 1)', 100) ORDER BY value;
SELECT tags, value FROM prometheusQuery(ts, 'vector(1 + 2)', 100);

SELECT '-- scalar results';
SELECT value FROM prometheusQuery(ts, '1 + 2 * 3', 100);
SELECT value FROM prometheusQuery(ts, '2 ^ 0.5', 100);
SELECT value FROM prometheusQuery(ts, '-7 % 3', 100);
SELECT value FROM prometheusQuery(ts, '7.5 % -2', 100);
SELECT value FROM prometheusQuery(ts, '5 % Inf', 100);
SELECT value FROM prometheusQuery(ts, 'Inf % 5', 100);
SELECT value FROM prometheusQuery(ts, '1 / 0', 100);
SELECT value FROM prometheusQuery(ts, '-1 / 0', 100);
SELECT value FROM prometheusQuery(ts, 'Inf - Inf', 100);
SELECT value FROM prometheusQuery(ts, '1 atan2 1', 100);
SELECT value FROM prometheusQuery(ts, '-(1 + 2)', 100);
SELECT value FROM prometheusQuery(ts, 'NaN > bool 1', 100);
SELECT value FROM prometheusQuery(ts, 'NaN != bool NaN', 100);
SELECT value FROM prometheusQuery(ts, '2 == bool 2', 100);
SELECT value FROM prometheusQuery(ts, '2 <= bool 1', 100);
SELECT value FROM prometheusQuery(ts, 'time() - 40', 100);

SELECT '-- range query';
SELECT arrayMap(x -> (toUnixTimestamp64Second(x.1), x.2), samples) FROM prometheusQueryRange(ts, '1 + 2', 100, 102, 1);
SELECT arrayMap(x -> (toUnixTimestamp64Second(x.1), x.2), samples) FROM prometheusQueryRange(ts, 'time() - 100 + 1', 100, 102, 1);

SELECT '-- a comparison of scalars still needs bool';
SELECT value FROM prometheusQuery(ts, '1 > 2', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SELECT '-- vector matching needs two instant vectors';
SELECT value FROM prometheusQuery(ts, '1 + on() 2', 100);
SELECT value FROM prometheusQuery(ts, '1 + on(foo) 2', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT value FROM prometheusQuery(ts, '1 + ignoring(foo) 2', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT value FROM prometheusQuery(ts, '1 == bool on(foo) 2', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT tags, value FROM prometheusQuery(ts, 'histogram_quantile(1 + on(foo) 0, req_bucket)', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

DROP TABLE ts;
