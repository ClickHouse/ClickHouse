-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
--
-- A range function over a range vector made from a scalar, for example `last_over_time(vector(1)[1500ms:3s])`, can have
-- no sample in some windows: a subquery is evaluated at multiples of its step, and a window shorter than the step
-- can fall between two of them. Prometheus omits such points, and so does ClickHouse, also when the result is passed
-- to another function.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

-- The subquery is evaluated at 999, 1002, ..., 1020. The windows at 1000, 1005, 1015 and 1020 contain a sample,
-- the window (1008.5, 1010] doesn't.
SELECT '-- last_over_time';
SELECT * FROM prometheusQueryRange(ts, 'last_over_time(vector(1)[1500ms:3s])', 1000, 1020, 5);

SELECT '-- instant query at a point with and without a sample';
SELECT * FROM prometheusQuery(ts, 'last_over_time(vector(1)[1500ms:3s])', 1005);
SELECT * FROM prometheusQuery(ts, 'last_over_time(vector(1)[1500ms:3s])', 1010);

SELECT '-- a fixed @ modifier freezes the window at a point without a sample';
SELECT * FROM prometheusQueryRange(ts, 'last_over_time(vector(1)[1500ms:3s] @ 1010)', 1000, 1020, 5);

SELECT '-- quantile_over_time';
SELECT * FROM prometheusQueryRange(ts, 'quantile_over_time(0.5, vector(1)[1500ms:3s])', 1000, 1020, 5);

-- The windows at 1005 and 1020 contain two samples, the other windows contain one sample, which is not enough for a regression.
SELECT '-- predict_linear';
SELECT * FROM prometheusQueryRange(ts, 'predict_linear(vector(1)[3500ms:3s], 10)', 1000, 1020, 5);

SELECT '-- the result of a range function in another range function';
SELECT * FROM prometheusQueryRange(ts, 'last_over_time(last_over_time(vector(1)[1500ms:3s])[20s:5s])', 1000, 1020, 5);

SELECT '-- scalar() of a vector is NaN where the vector has no sample';
SELECT * FROM prometheusQueryRange(ts, 'scalar(last_over_time(vector(1)[1500ms:3s]))', 1000, 1020, 5);

SELECT '-- absent_over_time';
SELECT * FROM prometheusQueryRange(ts, 'absent_over_time(vector(1)[1500ms:3s])', 1000, 1020, 5);

SELECT '-- aggregation and arithmetic over the result';
SELECT * FROM prometheusQueryRange(ts, 'sum(last_over_time(vector(1)[1500ms:3s]))', 1000, 1020, 5);
SELECT * FROM prometheusQueryRange(ts, 'last_over_time(vector(1)[1500ms:3s]) + 1', 1000, 1020, 5);

DROP TABLE ts;
