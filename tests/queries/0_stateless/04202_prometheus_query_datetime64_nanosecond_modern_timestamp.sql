-- Tags: no-fasttest, no-replicated-database
-- ^^ ANTLR4 support is disabled in the fast-test build, and the PromQL
-- grammar requires it. The experimental TimeSeries table engine does not
-- round-trip through DatabaseReplicated.

-- Regression test for `timeSeriesTimestampToAST`. With `DateTime64(9)` and a
-- modern Unix timestamp the raw value (e.g. 1.7e18 for 2024-01-01) exceeds
-- the precision of `Decimal64` (18 digits), so the timestamp literal must be
-- wrapped via `toDecimal128`, not `toDecimal64`, before conversion to
-- `DateTime64`. Otherwise `prometheusQuery` throws `DECIMAL_OVERFLOW`.

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
ENGINE = AggregatingMergeTree ORDER BY (metric_name, id);

CREATE TABLE ts_metrics (
    metric_family_name String,
    type String,
    unit String,
    help String)
ENGINE = ReplacingMergeTree ORDER BY metric_family_name;

CREATE TABLE ts_ns ENGINE = TimeSeries
DATA ts_data TAGS ts_tags METRICS ts_metrics;

-- A modern Unix timestamp (2024-01-01) on a `DateTime64(9)` table.
-- Without the `toDecimal128` wrap this would throw `DECIMAL_OVERFLOW`.
SELECT timestamp, value FROM prometheusQuery('ts_ns', '1 + 2', 1704067200);

-- Far-future timestamp, well past `Decimal64` precision.
SELECT timestamp, value FROM prometheusQuery('ts_ns', '10 * 5', 4102444800);

-- Small timestamp still works (the path that needs decimal wrapping for the
-- year-vs-seconds ambiguity).
SELECT timestamp, value FROM prometheusQuery('ts_ns', '7', 1000);

DROP TABLE ts_ns;
DROP TABLE ts_metrics;
DROP TABLE ts_tags;
DROP TABLE ts_data;
