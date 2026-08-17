-- Tags: no-fasttest, no-replicated-database
-- ^^ ANTLR4 support is disabled in the fast-test build, and the PromQL
-- grammar requires it. The experimental TimeSeries table engine does not
-- round-trip through DatabaseReplicated.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_data;
DROP TABLE IF EXISTS ts_tags;
DROP TABLE IF EXISTS ts_metrics;
DROP TABLE IF EXISTS ts_ns;
DROP TABLE IF EXISTS ts_data3;
DROP TABLE IF EXISTS ts_tags3;
DROP TABLE IF EXISTS ts_metrics3;
DROP TABLE IF EXISTS ts_ms;

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

CREATE TABLE ts_data3 (id UUID, timestamp DateTime64(3, 'UTC'), value Float64)
ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE ts_tags3 (
    id UUID,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time SimpleAggregateFunction(min, Nullable(DateTime64(3, 'UTC'))),
    max_time SimpleAggregateFunction(max, Nullable(DateTime64(3, 'UTC'))))
ENGINE = AggregatingMergeTree ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1;

CREATE TABLE ts_metrics3 (
    metric_family_name String,
    type String,
    unit String,
    help String)
ENGINE = ReplacingMergeTree ORDER BY metric_family_name;

CREATE TABLE ts_ms ENGINE = TimeSeries
DATA ts_data3 TAGS ts_tags3 METRICS ts_metrics3;

-- Rescaling a decimal argument up to the table's timestamp scale must report
-- DECIMAL_OVERFLOW instead of wrapping around to an unrelated timestamp.

-- Instant timestamp, Decimal64 field: raw 10^13 ticks at scale 3, table scale 9.
SELECT timestamp, value FROM prometheusQuery('ts_ns', '1 + 2', toDecimal64(10000000000, 3)); -- { serverError DECIMAL_OVERFLOW }

-- The smallest scale-3 value whose scale-9 form exceeds Int64.
SELECT timestamp, value FROM prometheusQuery('ts_ns', '1 + 2', toDecimal64(9223372036.855, 3)); -- { serverError DECIMAL_OVERFLOW }

-- Range start and end.
SELECT * FROM prometheusQueryRange('ts_ns', '1 + 2', toDecimal64(10000000000, 3), 2000, 15); -- { serverError DECIMAL_OVERFLOW }
SELECT * FROM prometheusQueryRange('ts_ns', '1 + 2', 1000, toDecimal64(10000000000, 3), 15); -- { serverError DECIMAL_OVERFLOW }

-- Range step: the duration instantiation of the same conversion.
SELECT * FROM prometheusQueryRange('ts_ns', '1 + 2', 1000, 2000, toDecimal64(10000000000, 3)); -- { serverError DECIMAL_OVERFLOW }

-- Valid values must keep converting exactly, in all three scale directions.
SELECT timestamp, value FROM prometheusQuery('ts_ns', '1 + 2', toDecimal64(1704067200.000, 3));
SELECT timestamp, value FROM prometheusQuery('ts_ns', '1 + 2', toDecimal64(9223372036.854, 3));
SELECT timestamp, value FROM prometheusQuery('ts_ns', '1 + 2', toDecimal64(1704067200.123456789, 9));
SELECT timestamp, value FROM prometheusQuery('ts_ms', '1 + 2', toDecimal64(1704067200.123456789, 9));
SELECT timestamp, value FROM prometheusQuery('ts_ms', '1 + 2', toDecimal64(-1.500, 3));
SELECT timestamp, value FROM prometheusQuery('ts_ns', '1 + 2', toDecimal32(1704067.20, 2));
SELECT timestamp, value FROM prometheusQuery('ts_ns', '1 + 2', toDecimal32(-2.50, 2));

-- The offset rewrite rescales the offset to the timestamp scale as well.
SELECT count() FROM prometheusQuery('ts_ns', 'foo offset 1d', toDecimal64(1704067200.000, 3));
SELECT count() FROM prometheusQuery('ts_ms', 'foo offset 1d', toDecimal64(1704067200.000, 3));

DROP TABLE ts_ms;
DROP TABLE ts_metrics3;
DROP TABLE ts_tags3;
DROP TABLE ts_data3;
DROP TABLE ts_ns;
DROP TABLE ts_metrics;
DROP TABLE ts_tags;
DROP TABLE ts_data;
