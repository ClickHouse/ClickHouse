-- Tags: no-fasttest
-- PromQL needs ANTLR4, which is disabled in the fast-test build.

SET enable_time_series_table = 1;
SET enable_time_series_aggregate_functions = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

SELECT '-- basic comparisons and expressions';
SELECT value FROM prometheusQuery(ts, 'min_of(3, 5)', 100);
SELECT value FROM prometheusQuery(ts, 'min_of(5, 3)', 100);
SELECT value FROM prometheusQuery(ts, 'max_of(3, 5)', 100);
SELECT value FROM prometheusQuery(ts, 'min_of(-2, -5)', 100);
SELECT value FROM prometheusQuery(ts, 'max_of(1.25, -2.5)', 100);
SELECT value FROM prometheusQuery(ts, 'min_of(max_of(-2, 7), 5)', 100);
SELECT value FROM prometheusQuery(ts, 'max_of(2 + 3, 2 * 4)', 100);
SELECT tags, value FROM prometheusQuery(ts, 'vector(min_of(3, 5))', 100);
SELECT tags, value FROM prometheusQuery(ts, 'clamp_min(vector(1), max_of(2, 3))', 100);

SELECT '-- NaN and infinities';
SELECT value FROM prometheusQuery(ts, 'min_of(NaN, 3)', 100);
SELECT value FROM prometheusQuery(ts, 'min_of(3, NaN)', 100);
SELECT value FROM prometheusQuery(ts, 'max_of(NaN, 3)', 100);
SELECT value FROM prometheusQuery(ts, 'max_of(3, NaN)', 100);
SELECT value FROM prometheusQuery(ts, 'min_of(-Inf, Inf)', 100);
SELECT value FROM prometheusQuery(ts, 'max_of(-Inf, Inf)', 100);

SELECT '-- scalar grids';
SELECT arrayMap(x -> x.2, samples)
FROM prometheusQueryRange(ts, 'min_of(time() - 100, 2)', 100, 104, 1);
SELECT arrayMap(x -> x.2, samples)
FROM prometheusQueryRange(ts, 'max_of(2, time() - 100)', 100, 104, 1);
SELECT arrayMap(x -> x.2, samples)
FROM prometheusQueryRange(ts, 'min_of(time() - 100, 104 - time())', 100, 104, 1);
SELECT arrayMap(x -> x.2, samples)
FROM prometheusQueryRange(ts, 'min_of(3, 5)', 100, 104, 1);

SELECT '-- runtime scalar inputs';
INSERT INTO ts (metric_name, samples) VALUES
    ('lhs', [
        (toDateTime64(100, 3), 1.),
        (toDateTime64(101, 3), 3.),
        (toDateTime64(102, 3), 5.)]),
    ('rhs', [
        (toDateTime64(100, 3), 2.),
        (toDateTime64(101, 3), 2.),
        (toDateTime64(102, 3), 2.)]);
SELECT arrayMap(x -> x.2, samples)
FROM prometheusQueryRange(ts, 'min_of(scalar(lhs), scalar(rhs))', 100, 102, 1);
SELECT arrayMap(x -> x.2, samples)
FROM prometheusQueryRange(ts, 'max_of(scalar(lhs), scalar(rhs))', 100, 102, 1);

SELECT '-- empty and multiple-series scalar inputs';
SELECT value FROM prometheusQuery(ts, 'min_of(scalar(missing), 1)', 100);
SELECT value FROM prometheusQuery(ts, 'max_of(1, scalar({__name__=~"lhs|rhs"}))', 100);

SELECT '-- function names remain valid metric and label names';
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('min_of', map('max_of', 'yes'), [(toDateTime64(100, 3), 7)]),
    ('max_of', map('min_of', 'yes'), [(toDateTime64(100, 3), 8)]);
SELECT value FROM prometheusQuery(ts, 'min_of{max_of="yes"}', 100);
SELECT value FROM prometheusQuery(ts, 'max_of{min_of="yes"}', 100);

SELECT '-- argument count and type errors';
SELECT * FROM prometheusQuery(ts, 'min_of()', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(ts, 'min_of(1)', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(ts, 'min_of(1, 2, 3)', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(ts, 'min_of(vector(1), 2)', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(ts, 'max_of(1, vector(2))', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SELECT '-- Float32 results are Float64';
DROP TABLE IF EXISTS ts_float32;
CREATE TABLE ts_float32 (samples Array(Tuple(DateTime64(3, 'UTC'), Float32))) ENGINE = TimeSeries;
INSERT INTO ts_float32 (metric_name, samples)
SELECT metric_name, samples FROM ts WHERE metric_name IN ('lhs', 'rhs');
SELECT toTypeName(value), value FROM prometheusQuery(ts_float32, 'min_of(scalar(lhs), 2)', 101);
SELECT toTypeName(value), value FROM prometheusQuery(ts_float32, 'max_of(2, scalar(lhs))', 101);

DROP TABLE ts_float32;
DROP TABLE ts;
