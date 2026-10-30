-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Tests the PromQL function `quantile_over_time`, which is built on the aggregate function timeSeriesQuantileToGrid
-- (tested in 05212_timeseries_quantile_to_grid).

SET enable_time_series_table = 1;
SET enable_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

-- The evaluation time is 1700000000, the [3m] window (1699999820, 1700000000] holds three samples of every series:
-- host1 = {10, 20, 30}, host2 = {5, 15, 25}, host3 = {100, 100, 100}.
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('up', map('instance', 'host1'), [(toDateTime64(1699999880, 3), 10.0), (toDateTime64(1699999940, 3), 20.0), (toDateTime64(1700000000, 3), 30.0)]),
    ('up', map('instance', 'host2'), [(toDateTime64(1699999880, 3), 5.0), (toDateTime64(1699999940, 3), 15.0), (toDateTime64(1700000000, 3), 25.0)]),
    ('up', map('instance', 'host3'), [(toDateTime64(1699999880, 3), 100.0), (toDateTime64(1699999940, 3), 100.0), (toDateTime64(1700000000, 3), 100.0)]);

SELECT 'quantile_over_time(0.5, up[3m]): the median of the samples in the window';
SELECT tags, value FROM prometheusQuery(ts, 'quantile_over_time(0.5, up[3m])', 1700000000) ORDER BY ALL;

SELECT 'quantile_over_time(0, up[3m]) and quantile_over_time(1, up[3m]): the minimum and the maximum';
SELECT tags, value FROM prometheusQuery(ts, 'quantile_over_time(0, up[3m])', 1700000000) ORDER BY ALL;
SELECT tags, value FROM prometheusQuery(ts, 'quantile_over_time(1, up[3m])', 1700000000) ORDER BY ALL;

SELECT 'quantile_over_time(0.25, up[3m]): interpolated between the two lower samples';
SELECT tags, value FROM prometheusQuery(ts, 'quantile_over_time(0.25, up[3m])', 1700000000) ORDER BY ALL;

SELECT 'a level outside [0, 1] gives -Inf or +Inf and a NaN level gives NaN, like in Prometheus';
SELECT tags, value FROM prometheusQuery(ts, 'quantile_over_time(2, up[3m])', 1700000000) ORDER BY ALL;
SELECT tags, value FROM prometheusQuery(ts, 'quantile_over_time(-1, up[3m])', 1700000000) ORDER BY ALL;
SELECT tags, value FROM prometheusQuery(ts, 'quantile_over_time(NaN, up[3m])', 1700000000) ORDER BY ALL;

SELECT 'the level may be a scalar expression that is only known when the query runs';
SELECT tags, value FROM prometheusQuery(ts, 'quantile_over_time(scalar(sum(vector(0.5))), up[3m])', 1700000000) ORDER BY ALL;

-- The level is 0 at the step 1700000000 and 1 at the step 1700000001, and both windows hold the same samples.
SELECT 'the level may vary with the evaluation time: one level per step of a range query';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Second(x.1), x.2), samples) AS samples
FROM prometheusQueryRange(ts, 'quantile_over_time(time() - 1700000000, up[3m])', 1700000000, 1700000001, 1) ORDER BY ALL;

-- A fixed `@` modifier freezes the window at the `@` timestamp, and a quantile has no evaluation-time term, so PromQL
-- evaluates the call once and repeats the result over the steps. Every step below is past the last sample, so a
-- window sliding with the steps would give NULLs instead.
SELECT 'quantile_over_time with a fixed @, range query: the same quantile at every step';
SELECT tags, arrayMap(x -> x.2, samples) AS values
FROM prometheusQueryRange(ts, 'quantile_over_time(0.5, up[3m] @ 1700000000)', 1700000100, 1700000400, 100) ORDER BY ALL;

-- `timeSeriesQuantileToGrid` derives its window from each grid point and cannot express a frozen window with a
-- per-point quantile level, so this combination is rejected instead of returning sliding-window results.
SELECT * FROM prometheusQueryRange(ts, 'quantile_over_time(time(), up[3m] @ 1700000000)', 1700000100, 1700000400, 100); -- { serverError NOT_IMPLEMENTED }

DROP TABLE ts;
