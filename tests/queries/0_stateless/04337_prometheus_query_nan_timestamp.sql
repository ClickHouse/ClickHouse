-- Tags: no-fasttest, no-replicated-database
-- ^^ ANTLR4 support is disabled in the fast-test build, and the PromQL
-- grammar requires it. The experimental TimeSeries table engine does not
-- round-trip through DatabaseReplicated.

-- Regression test: a non-finite Float64 (NaN/inf) argument used as a
-- timestamp/duration in prometheusQuery / prometheusQueryRange must produce a
-- clean error instead of undefined behaviour. Previously a NaN bypassed the
-- overflow range check in getFromFloat (every comparison with NaN is false),
-- so static_cast<Int64>(NaN) executed - undefined behaviour caught by UBSan.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_data;
DROP TABLE IF EXISTS ts_tags;
DROP TABLE IF EXISTS ts_metrics;
DROP TABLE IF EXISTS ts_ns;

CREATE TABLE ts_data (id UUID, timestamp DateTime64(9, 'UTC'), value Float64)
ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE ts_tags (
    id UUID,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time SimpleAggregateFunction(min, Nullable(DateTime64(9, 'UTC'))),
    max_time SimpleAggregateFunction(max, Nullable(DateTime64(9, 'UTC'))))
-- `tags` is functionally dependent on `id`, so it is kept outside the sorting key on purpose.
ENGINE = AggregatingMergeTree ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1;

CREATE TABLE ts_metrics (
    metric_family_name String,
    type String,
    unit String,
    help String)
ENGINE = ReplacingMergeTree ORDER BY metric_family_name;

CREATE TABLE ts_ns ENGINE = TimeSeries
DATA ts_data TAGS ts_tags METRICS ts_metrics;

-- Timestamp path: getFromFloat<DateTime64>.
SELECT timestamp, value FROM prometheusQuery('ts_ns', '1 + 2', nan); -- { serverError BAD_ARGUMENTS }
SELECT timestamp, value FROM prometheusQuery('ts_ns', '1 + 2', -nan); -- { serverError BAD_ARGUMENTS }
SELECT timestamp, value FROM prometheusQuery('ts_ns', '1 + 2', inf); -- { serverError BAD_ARGUMENTS }
SELECT timestamp, value FROM prometheusQuery('ts_ns', '1 + 2', -inf); -- { serverError BAD_ARGUMENTS }
SELECT timestamp, value FROM prometheusQuery('ts_ns', '1 + 2', 0. / 0.); -- { serverError BAD_ARGUMENTS }

-- Duration path: getFromFloat<Decimal64> (the step argument of prometheusQueryRange).
SELECT timestamp, value FROM prometheusQueryRange('ts_ns', '1 + 2', 1000, 2000, nan); -- { serverError BAD_ARGUMENTS }
SELECT timestamp, value FROM prometheusQueryRange('ts_ns', '1 + 2', 1000, 2000, inf); -- { serverError BAD_ARGUMENTS }

-- A finite float timestamp still works (sanity: the guard does not reject valid input).
SELECT timestamp, value FROM prometheusQuery('ts_ns', '1 + 2', 1704067200.0);

DROP TABLE ts_ns;
DROP TABLE ts_metrics;
DROP TABLE ts_tags;
DROP TABLE ts_data;
