-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-replicated-database: the experimental TimeSeries table engine does not round-trip through DatabaseReplicated.

-- The `locality_hash` column is optional: tables created by older versions of ClickHouse don't have it.
-- A TimeSeries table over such tables must still be creatable, writable, and queryable with PromQL -
-- the engine just doesn't fill or use the column then.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

-- Case 1: neither the samples nor the tags table has `locality_hash` (the pre-locality_hash schema).
CREATE TABLE old_samples (id UUID, timestamp DateTime64(3), value Float64)
    ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE TABLE old_tags (id UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)), metric_name LowCardinality(String), tags Map(LowCardinality(String), String), all_tags Map(String, String) EPHEMERAL, min_time SimpleAggregateFunction(min, Nullable(DateTime64(3))), max_time SimpleAggregateFunction(max, Nullable(DateTime64(3))))
    ENGINE = AggregatingMergeTree ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1;
CREATE TABLE old_metrics (metric_family_name String, type String, unit String, help String)
    ENGINE = ReplacingMergeTree ORDER BY metric_family_name;

CREATE TABLE ts_old ENGINE = TimeSeries SAMPLES old_samples TAGS old_tags METRICS old_metrics;

INSERT INTO ts_old (metric_name, tags, time_series) VALUES
    ('http_requests_count', map('job', 'web'), [(toDateTime64(1000, 3), 1.), (toDateTime64(1010, 3), 2.)]),
    ('cpu_usage', map('job', 'web'), [(toDateTime64(1010, 3), 50.)]);

SELECT 'no locality_hash at all:';
SELECT * FROM prometheusQuery(ts_old, 'http_requests_count', toDateTime64(1010, 3)) ORDER BY tags;
SELECT * FROM prometheusQuery(ts_old, 'sum(cpu_usage)', toDateTime64(1010, 3));

-- The query generated for a PromQL selector falls back to filtering by `id` alone.
SELECT 'primary key usage without locality_hash:';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1 SELECT * FROM timeSeriesSelector(ts_old, 'http_requests_count', toDateTime64(0, 3), toDateTime64(2000, 3))
) WHERE explain LIKE '%Keys:%' OR trimBoth(explain) IN ('locality_hash', 'id', 'timestamp');

DROP TABLE ts_old;
DROP TABLE old_samples;
DROP TABLE old_tags;
DROP TABLE old_metrics;

-- Case 2: the samples table has `locality_hash` but the tags table doesn't.
-- The write path fills the column, the read path doesn't use it.
CREATE TABLE new_samples (locality_hash UInt64, id UUID, timestamp DateTime64(3), value Float64)
    ENGINE = MergeTree ORDER BY (locality_hash, id, timestamp);
CREATE TABLE old_tags (id UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)), metric_name LowCardinality(String), tags Map(LowCardinality(String), String), all_tags Map(String, String) EPHEMERAL, min_time SimpleAggregateFunction(min, Nullable(DateTime64(3))), max_time SimpleAggregateFunction(max, Nullable(DateTime64(3))))
    ENGINE = AggregatingMergeTree ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1;
CREATE TABLE old_metrics (metric_family_name String, type String, unit String, help String)
    ENGINE = ReplacingMergeTree ORDER BY metric_family_name;

CREATE TABLE ts_mixed ENGINE = TimeSeries SAMPLES new_samples TAGS old_tags METRICS old_metrics;

INSERT INTO ts_mixed (metric_name, tags, time_series) VALUES
    ('http_requests_count', map('job', 'web'), [(toDateTime64(1000, 3), 1.)]);

SELECT 'samples with locality_hash, tags without:';
SELECT countIf(s.locality_hash = xxHash64(t.metric_name)), count()
FROM timeSeriesSamples(ts_mixed) AS s
JOIN timeSeriesTags(ts_mixed) AS t ON s.id = t.id;
SELECT * FROM prometheusQuery(ts_mixed, 'http_requests_count', toDateTime64(1000, 3)) ORDER BY tags;

DROP TABLE ts_mixed;
DROP TABLE new_samples;
DROP TABLE old_tags;
DROP TABLE old_metrics;

-- Case 3: the tags table has `locality_hash` but the samples table doesn't.
-- The read path must not use the column because the samples table can't be filtered by it.
CREATE TABLE old_samples (id UUID, timestamp DateTime64(3), value Float64)
    ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE TABLE new_tags (id UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)), metric_name LowCardinality(String), locality_hash UInt64 MATERIALIZED xxHash64(metric_name), tags Map(LowCardinality(String), String), all_tags Map(String, String) EPHEMERAL, min_time SimpleAggregateFunction(min, Nullable(DateTime64(3))), max_time SimpleAggregateFunction(max, Nullable(DateTime64(3))))
    ENGINE = AggregatingMergeTree ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1;
CREATE TABLE old_metrics (metric_family_name String, type String, unit String, help String)
    ENGINE = ReplacingMergeTree ORDER BY metric_family_name;

CREATE TABLE ts_mixed2 ENGINE = TimeSeries SAMPLES old_samples TAGS new_tags METRICS old_metrics;

INSERT INTO ts_mixed2 (metric_name, tags, time_series) VALUES
    ('http_requests_count', map('job', 'web'), [(toDateTime64(1000, 3), 1.)]);

SELECT 'tags with locality_hash, samples without:';
SELECT * FROM prometheusQuery(ts_mixed2, 'http_requests_count', toDateTime64(1000, 3)) ORDER BY tags;

DROP TABLE ts_mixed2;
DROP TABLE old_samples;
DROP TABLE new_tags;
DROP TABLE old_metrics;
