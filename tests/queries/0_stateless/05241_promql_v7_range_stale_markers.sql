-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-replicated-database: deferred drops of `TimeSeries` inner tables are incompatible with replicated databases.

SET allow_experimental_time_series_table = 1;
SET enable_promql_native_plan = 0;
SET session_timezone = 'UTC';

CREATE TABLE promql_v7_range_stale_markers
ENGINE = TimeSeries
SETTINGS version = 7, recent_samples_ttl_seconds = 0;

INSERT INTO promql_v7_range_stale_markers (metric_name, tags, samples)
SELECT 'm', map('job', 'api'),
    [(toDateTime64(90, 3), 1.),
     (toDateTime64(95, 3), reinterpretAsFloat64(toUInt64(0x7FF0000000000002))),
     (toDateTime64(100, 3), reinterpretAsFloat64(toUInt64(0x7FF8000000000001)))];

-- The stale NaN is omitted from a range selector; an ordinary NaN stays.
SELECT
    length(samples),
    arrayCount(sample -> reinterpretAsUInt64(sample.2) = toUInt64(0x7FF0000000000002), samples),
    arrayCount(sample -> reinterpretAsUInt64(sample.2) = toUInt64(0x7FF8000000000001), samples)
FROM prometheusQuery(promql_v7_range_stale_markers, 'm[20s]', 100);

DROP TABLE promql_v7_range_stale_markers SYNC;

-- The older row layout keeps the same semantics.
CREATE TABLE promql_v6_range_stale_markers
ENGINE = TimeSeries
SETTINGS version = 6, recent_samples_ttl_seconds = 0;

INSERT INTO promql_v6_range_stale_markers (metric_name, tags, samples)
SELECT 'm', map('job', 'api'),
    [(toDateTime64(90, 3), 1.),
     (toDateTime64(95, 3), reinterpretAsFloat64(toUInt64(0x7FF0000000000002))),
     (toDateTime64(100, 3), reinterpretAsFloat64(toUInt64(0x7FF8000000000001)))];

SELECT
    length(samples),
    arrayCount(sample -> reinterpretAsUInt64(sample.2) = toUInt64(0x7FF0000000000002), samples),
    arrayCount(sample -> reinterpretAsUInt64(sample.2) = toUInt64(0x7FF8000000000001), samples)
FROM prometheusQuery(promql_v6_range_stale_markers, 'm[20s]', 100);

DROP TABLE promql_v6_range_stale_markers SYNC;
