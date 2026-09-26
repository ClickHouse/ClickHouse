-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Tests the PromQL function `predict_linear`, which is built on the aggregate function timeSeriesLinearRegressionToGrid
-- (tested in 05214_timeseries_linear_regression_to_grid).

SET enable_time_series_table = 1;
SET enable_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

-- The evaluation time is 1700000000, the [3m] window (1699999820, 1700000000] holds three samples of every series:
-- host1 and host2 are linear ramps with the slope 1/6 per second ending at 30 and 25, host3 is constant.
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('up', map('instance', 'host1'), [(toDateTime64(1699999880, 3), 10.0), (toDateTime64(1699999940, 3), 20.0), (toDateTime64(1700000000, 3), 30.0)]),
    ('up', map('instance', 'host2'), [(toDateTime64(1699999880, 3), 5.0), (toDateTime64(1699999940, 3), 15.0), (toDateTime64(1700000000, 3), 25.0)]),
    ('up', map('instance', 'host3'), [(toDateTime64(1699999880, 3), 100.0), (toDateTime64(1699999940, 3), 100.0), (toDateTime64(1700000000, 3), 100.0)]);

SELECT 'predict_linear(up[3m], 0): the fitted value at the evaluation time';
SELECT tags, value FROM prometheusQuery(ts, 'predict_linear(up[3m], 0)', 1700000000) ORDER BY ALL;

SELECT 'predict_linear(up[3m], 60): extrapolated 60 seconds past the evaluation time, 30 + 60/6 and 25 + 60/6';
SELECT tags, value FROM prometheusQuery(ts, 'predict_linear(up[3m], 60)', 1700000000) ORDER BY ALL;

-- At the step 1699999940 the window (1699999760, 1699999940] holds two samples, the fit gives 20 there.
SELECT 'predict_linear, range query: the window slides with the steps';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Second(x.1), x.2), samples) AS samples
FROM prometheusQueryRange(ts, 'predict_linear(up{instance="host1"}[3m], 60)', 1699999940, 1700000000, 60);

SELECT 'the horizon may be a scalar expression that is only known when the query runs';
SELECT tags, value FROM prometheusQuery(ts, 'predict_linear(up[3m], scalar(sum(vector(60))))', 1700000000) ORDER BY ALL;

-- The horizon is the evaluation time itself, so the prediction is 30 + 1700000000/6, whatever the spelling.
SELECT 'the horizon may depend on the evaluation time';
SELECT tags, value FROM prometheusQuery(ts, 'predict_linear(up{instance="host1"}[3m], time())', 1700000000);
SELECT tags, value FROM prometheusQuery(ts, 'predict_linear(up{instance="host1"}[3m], scalar(sum(vector(time()))))', 1700000000);

-- A fixed `@` modifier on the range vector freezes the sample window at the `@` timestamp. For most range functions
-- the whole call is then step-invariant and PromQL evaluates it once. `predict_linear` is not: its result depends on
-- the evaluation time, so Prometheus lists it among the functions unsafe under `@` and evaluates it at every step
-- against the frozen window. The prediction at the step `t` is the fit at the frozen timestamp, extrapolated to `t`
-- and then by the horizon. Every step below is past the last sample, so a window sliding with the steps would give
-- NULLs instead.

-- The window is frozen at 1700000000, and the prediction at the step `t` is 30 + (t - 1700000000 + 60) / 6.
SELECT 'predict_linear with a fixed @, range query: the frozen fit extrapolated to every step';
SELECT tags, arrayMap(x -> round(x.2, 3), samples) AS values
FROM prometheusQueryRange(ts, 'predict_linear(up{instance="host1"}[3m] @ 1700000000, 60)', 1700000100, 1700000400, 100);

-- Evaluated exactly at the `@` timestamp, so window time and evaluation time coincide: 30 + 60/6 = 40.
SELECT 'predict_linear with a fixed @ at the evaluation time: no shift';
SELECT tags, value FROM prometheusQuery(ts, 'predict_linear(up{instance="host1"}[3m] @ 1700000000, 60)', 1700000000);

-- Same frozen window, evaluated 100 seconds later: 30 + (100 + 60) / 6.
SELECT 'predict_linear with a fixed @, instant query 100 seconds later';
SELECT tags, value FROM prometheusQuery(ts, 'predict_linear(up{instance="host1"}[3m] @ 1700000000, 60)', 1700000100);

-- The horizon may vary with the step as well. `60 + 0 * time()` is 60 at every step (it varies with `time()` only
-- formally) and must give the same result as the constant 60 above; `time()` is the step itself.
SELECT 'predict_linear with a fixed @ and a varying horizon: per-step predictions from the frozen fit';
SELECT tags, arrayMap(x -> round(x.2, 3), samples) AS values
FROM prometheusQueryRange(ts, 'predict_linear(up{instance="host1"}[3m] @ 1700000000, 60 + 0 * time())', 1700000100, 1700000400, 100);
SELECT tags, arrayMap(x -> round(x.2, 3), samples) AS values
FROM prometheusQueryRange(ts, 'predict_linear(up{instance="host1"}[3m] @ 1700000000, time())', 1700000100, 1700000400, 100);

DROP TABLE ts;
