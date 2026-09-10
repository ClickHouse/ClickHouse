-- Tags: no-fasttest
-- ^^ ANTLR4 support is disabled in the fast-test build, and the PromQL
-- grammar requires it.

-- Regression test: `offset` inside a range selector (e.g. `rate(m[2m] offset 5m)`)
-- used to fail with "Illegal type Decimal(18, 0) of argument of function
-- toIntervalNanosecond": the transpiled `timestamp + INTERVAL x` expression always
-- picked the nanosecond interval regardless of the timestamp scale, and passed the
-- offset as a Decimal literal, which the interval functions do not accept.

SET allow_experimental_time_series_table = 1;
SET allow_experimental_time_series_aggregate_functions = 1;
SET session_timezone = 'UTC'; -- the reference contains rendered DateTime64 values

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

INSERT INTO ts (metric_name, tags, time_series) VALUES
    ('m', map('l', 'a'), [(toDateTime64(1000000, 3), 1.0), (toDateTime64(1000060, 3), 2.0), (toDateTime64(1000120, 3), 3.0)]);

-- The failing case: offset in a range selector, instant query (default timestamps are DateTime64(3)).
SELECT 'rate with offset, instant:';
SELECT tags, timestamp, value FROM prometheusQuery(ts, 'rate(m[2m] offset 5m)', 1000420) ORDER BY ALL;

-- `rate(m[2m] offset 5m)` evaluated at T must equal `rate(m[2m])` evaluated at T - 300.
SELECT 'offset is equivalent to shifting the evaluation time:';
SELECT (SELECT groupArray(value) FROM prometheusQuery(ts, 'rate(m[2m] offset 5m)', 1000420))
     = (SELECT groupArray(value) FROM prometheusQuery(ts, 'rate(m[2m])', 1000120));

SELECT 'increase with offset, range:';
SELECT tags, time_series FROM prometheusQueryRange(ts, 'increase(m[2m] offset 1m)', 1000180, 1000300, 60) ORDER BY ALL;

-- Instant-vector offset took a different code path and worked before; keep it covered.
SELECT 'plain vector offset, range:';
SELECT tags, time_series FROM prometheusQueryRange(ts, 'm offset 5m', 1000300, 1000400, 100) ORDER BY ALL;

DROP TABLE ts;

-- Nanosecond timestamps exercise the toIntervalNanosecond branch, including a sub-second offset.
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

INSERT INTO ts_ns (metric_name, tags, time_series) VALUES
    ('n', map(), [(toDateTime64('1970-01-12 13:46:40.000000001', 9, 'UTC'), 10.0), (toDateTime64('1970-01-12 13:47:40.000000001', 9, 'UTC'), 20.0)]);

SELECT 'nanosecond timestamps, sub-second offset:';
SELECT tags, timestamp, value FROM prometheusQuery(ts_ns, 'last_over_time(n[2m] offset 500ms)', 1000160.5) ORDER BY ALL;

DROP TABLE ts_ns;
DROP TABLE ts_metrics;
DROP TABLE ts_tags;
DROP TABLE ts_data;
