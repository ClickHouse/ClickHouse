-- Tags: no-fasttest, no-replicated-database
-- ^^ ANTLR4 support is disabled in the fast-test build, and the PromQL
-- grammar requires it. The experimental TimeSeries table engine does not
-- round-trip through DatabaseReplicated.

-- Regression test: a fixed `@` modifier on the range vector makes the whole call step-invariant in
-- PromQL, so `predict_linear` / `quantile_over_time` must be evaluated once, at the fixed timestamp,
-- and that single result repeated over the outer range-query grid (as the shared `rate`/`increase`/...
-- path already does). Both used to slide the aggregate window over the outer evaluation timestamps
-- instead, so the result changed from step to step and eventually became NULL.

SET allow_experimental_time_series_table = 1;
SET allow_experimental_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

-- Linear ramp 10 -> 20 -> 30 (slope +1/6 per second), ending at the `@` timestamp 1700000000.
INSERT INTO ts (metric_name, tags, time_series) VALUES
    ('up', map('instance', 'host1'), [(toDateTime64(1699999880, 3), 10.0), (toDateTime64(1699999940, 3), 20.0), (toDateTime64(1700000000, 3), 30.0)]);

-- Every grid point of the range below is past the last sample, so without the fixed-@ handling the
-- window slides off the samples and the result decays to a single non-NULL step (or none at all).

SELECT 'predict_linear with a fixed @, range query: the value at 1700000000 (30 + 60/6 = 40), repeated:';
SELECT tags, arrayMap(x -> x.2, time_series) AS values
FROM prometheusQueryRange(ts, 'predict_linear(up[3m] @ 1700000000, 60)', 1700000100, 1700000400, 100)
ORDER BY ALL;

SELECT 'predict_linear with a fixed @: exactly one distinct value across all steps:';
SELECT length(arrayDistinct(arrayMap(x -> x.2, time_series)))
FROM prometheusQueryRange(ts, 'predict_linear(up[3m] @ 1700000000, 60)', 1700000100, 1700000400, 100)
ORDER BY ALL;

SELECT 'quantile_over_time with a fixed @, range query: the median of 10/20/30, repeated:';
SELECT tags, arrayMap(x -> x.2, time_series) AS values
FROM prometheusQueryRange(ts, 'quantile_over_time(0.5, up[3m] @ 1700000000)', 1700000100, 1700000400, 100)
ORDER BY ALL;

SELECT 'quantile_over_time with a fixed @: exactly one distinct value across all steps:';
SELECT length(arrayDistinct(arrayMap(x -> x.2, time_series)))
FROM prometheusQueryRange(ts, 'quantile_over_time(0.5, up[3m] @ 1700000000)', 1700000100, 1700000400, 100)
ORDER BY ALL;

-- A fixed `@` freezes the samples but not the other argument, so with a per-step-varying scalar the
-- call is no longer step-invariant and PromQL keeps evaluating it per step against the frozen window.
-- The `timeSeries*VaryingToGrid` aggregates derive their window from each grid point and cannot
-- express that, so the combination is rejected instead of returning sliding-window results.
SELECT * FROM prometheusQueryRange(ts, 'predict_linear(up[3m] @ 1700000000, time())', 1700000100, 1700000400, 100); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM prometheusQueryRange(ts, 'quantile_over_time(time(), up[3m] @ 1700000000)', 1700000100, 1700000400, 100); -- { serverError NOT_IMPLEMENTED }

DROP TABLE ts;
