-- Tags: no-fasttest, no-replicated-database
-- ^^ ANTLR4 support is disabled in the fast-test build, and the PromQL
-- grammar requires it. The experimental TimeSeries table engine does not
-- round-trip through DatabaseReplicated.

SET allow_experimental_time_series_table = 1;
SET allow_experimental_time_series_aggregate_functions = 1;
SET session_timezone = 'UTC'; -- the reference contains rendered DateTime64 values

DROP TABLE IF EXISTS ts_data;
DROP TABLE IF EXISTS ts_tags;
DROP TABLE IF EXISTS ts_metrics;
DROP TABLE IF EXISTS ts_ns;
DROP TABLE IF EXISTS ts_data3;
DROP TABLE IF EXISTS ts_tags3;
DROP TABLE IF EXISTS ts_metrics3;
DROP TABLE IF EXISTS ts_ms;
DROP TABLE IF EXISTS ts_data4;
DROP TABLE IF EXISTS ts_tags4;
DROP TABLE IF EXISTS ts_metrics4;
DROP TABLE IF EXISTS ts_us;

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

-- Timestamp scale 4 is not a multiple of 3, so the offset rewrite rounds the interval
-- scale up to 6 and genuinely rescales 4 -> 6.
CREATE TABLE ts_data4 (id UUID, timestamp DateTime64(4, 'UTC'), value Float64)
ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE ts_tags4 (
    id UUID,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time SimpleAggregateFunction(min, Nullable(DateTime64(4, 'UTC'))),
    max_time SimpleAggregateFunction(max, Nullable(DateTime64(4, 'UTC'))))
ENGINE = AggregatingMergeTree ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1;

CREATE TABLE ts_metrics4 (
    metric_family_name String,
    type String,
    unit String,
    help String)
ENGINE = ReplacingMergeTree ORDER BY metric_family_name;

CREATE TABLE ts_us ENGINE = TimeSeries
DATA ts_data4 TAGS ts_tags4 METRICS ts_metrics4;

-- The last two samples sit 0.9 s apart, so a sub-second offset that is rescaled by the
-- wrong factor selects a different sample.
INSERT INTO ts_us (metric_name, tags, time_series) VALUES
    ('m4', map('l', 'a'), [(toDateTime64(1000000, 4), 1.0), (toDateTime64(1000060, 4), 2.0), (toDateTime64(1000120, 4), 3.0),
                           (toDateTime64('1970-01-12 13:50:00.5000', 4, 'UTC'), 9.0),
                           (toDateTime64('1970-01-12 13:50:01.4000', 4, 'UTC'), 11.0)]);

INSERT INTO ts_ns (metric_name, tags, time_series) VALUES
    ('foo', map('l', 'a'), [(toDateTime64('2023-12-31 00:00:00', 9, 'UTC'), 7.0)]);

INSERT INTO ts_ms (metric_name, tags, time_series) VALUES
    ('foo', map('l', 'a'), [(toDateTime64('2023-12-31 00:00:00', 3, 'UTC'), 7.0)]);

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

-- An `offset` inside a range selector goes through the same rescaling, from the
-- timestamp scale up to the interval scale. Assert the shifted timestamps, so a
-- wrong rescale changes the result instead of passing silently.
SELECT 'offset 1m, scale 4 rescaled to 6:';
SELECT tags, timestamp, value FROM prometheusQuery(ts_us, 'last_over_time(m4[30s] offset 1m)', 1000180) ORDER BY ALL;

SELECT 'sub-second offset, scale 4 rescaled to 6:';
SELECT tags, timestamp, value FROM prometheusQuery(ts_us, 'last_over_time(m4[1s] offset 500ms)', 1000201.6) ORDER BY ALL;

-- `offset D` evaluated at T must equal no offset evaluated at T - D. Self-checking,
-- so it fails on any mis-rescale without depending on hand-computed constants.
SELECT 'offset is equivalent to shifting the evaluation time:';
SELECT (SELECT groupArray(value) FROM prometheusQuery(ts_us, 'last_over_time(m4[30s] offset 1m)', 1000180))
     = (SELECT groupArray(value) FROM prometheusQuery(ts_us, 'last_over_time(m4[30s])', 1000120));
SELECT (SELECT groupArray(value) FROM prometheusQuery(ts_us, 'last_over_time(m4[1s] offset 500ms)', 1000201.6))
     = (SELECT groupArray(value) FROM prometheusQuery(ts_us, 'last_over_time(m4[1s])', 1000201.1));

SELECT 'range query with offset, scale 4:';
SELECT tags, time_series FROM prometheusQueryRange(ts_us, 'last_over_time(m4[1s] offset 500ms)', 1000201.6, 1000202.6, 1) ORDER BY ALL;

-- Multiples of 3 take the no-op branch of the same conversion; keep them covered.
SELECT 'offset with a scale that needs no rescaling:';
SELECT tags, timestamp, value FROM prometheusQuery('ts_ns', 'last_over_time(foo[30s] offset 1d)', toDecimal64(1704067200.000, 3)) ORDER BY ALL;
SELECT tags, timestamp, value FROM prometheusQuery('ts_ms', 'last_over_time(foo[30s] offset 1d)', toDecimal64(1704067200.000, 3)) ORDER BY ALL;
SELECT (SELECT groupArray(value) FROM prometheusQuery('ts_ns', 'last_over_time(foo[30s] offset 1d)', toDecimal64(1704067200.000, 3)))
     = (SELECT groupArray(value) FROM prometheusQuery('ts_ns', 'last_over_time(foo[30s])', toDecimal64(1703980800.000, 3)));

DROP TABLE ts_us;
DROP TABLE ts_metrics4;
DROP TABLE ts_tags4;
DROP TABLE ts_data4;
DROP TABLE ts_ms;
DROP TABLE ts_metrics3;
DROP TABLE ts_tags3;
DROP TABLE ts_data3;
DROP TABLE ts_ns;
DROP TABLE ts_metrics;
DROP TABLE ts_tags;
DROP TABLE ts_data;
