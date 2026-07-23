-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-replicated-database: the experimental TimeSeries table engine does not round-trip through DatabaseReplicated.

-- The samples inner table of a TimeSeries table has a `locality_hash` column (`xxHash64(metric_name)`)
-- as the first column of its primary key, so samples of the same metric are stored close to each other.
-- The tags inner table has the same column MATERIALIZED, and the SQL queries generated for evaluating
-- PromQL selectors use the condition `(locality_hash, id) IN (SELECT locality_hash, ... FROM tags)`
-- to prune granules by the primary key index of the samples table.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_locality;
CREATE TABLE ts_locality ENGINE = TimeSeries;

SELECT 'samples inner table columns:';
SELECT name, type, default_kind, default_expression FROM system.columns
WHERE database = currentDatabase() AND table LIKE '.inner_id.samples.%' ORDER BY position;

SELECT 'samples inner table engine:';
SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.samples.%';

SELECT 'tags inner table locality_hash column:';
SELECT name, type, default_kind, default_expression FROM system.columns
WHERE database = currentDatabase() AND table LIKE '.inner_id.tags.%' AND name = 'locality_hash';

-- Insert a few time series through the TimeSeries table.
INSERT INTO ts_locality (metric_name, tags, time_series) VALUES
    ('http_requests_count', map('job', 'web', 'instance', 'host1'), [(toDateTime64(1000, 3), 1.), (toDateTime64(1010, 3), 2.)]),
    ('http_requests_count', map('job', 'web', 'instance', 'host2'), [(toDateTime64(1010, 3), 3.)]),
    ('cpu_usage', map('job', 'web'), [(toDateTime64(1010, 3), 50.)]);

-- `locality_hash` must be `xxHash64(metric_name)` in both inner tables.
SELECT 'locality_hash invariant in the tags table:';
SELECT countIf(locality_hash = xxHash64(metric_name)), count() FROM timeSeriesTags(ts_locality);

SELECT 'locality_hash invariant in the samples table:';
SELECT countIf(s.locality_hash = xxHash64(t.metric_name)), count()
FROM timeSeriesSamples(ts_locality) AS s
JOIN timeSeriesTags(ts_locality) AS t ON s.id = t.id;

-- PromQL queries evaluate correctly with the `(locality_hash, id) IN (...)` condition.
SELECT 'prometheusQuery:';
SELECT * FROM prometheusQuery(ts_locality, 'http_requests_count', toDateTime64(1010, 3)) ORDER BY tags;
SELECT * FROM prometheusQuery(ts_locality, 'sum(http_requests_count)', toDateTime64(1010, 3));

-- The query generated for a PromQL selector uses the primary key index of the samples table,
-- with `locality_hash` as the first key column.
SELECT 'primary key usage:';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1 SELECT * FROM timeSeriesSelector(ts_locality, 'http_requests_count', toDateTime64(0, 3), toDateTime64(2000, 3))
) WHERE explain LIKE '%Keys:%' OR explain LIKE '%Condition:%' OR trimBoth(explain) IN ('locality_hash', 'id', 'timestamp');

-- Wrong types of the `locality_hash` column are rejected.
CREATE TABLE ts_bad_samples_locality ENGINE = TimeSeries SAMPLES INNER COLUMNS (locality_hash String); -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE ts_bad_tags_locality ENGINE = TimeSeries TAGS INNER COLUMNS (locality_hash String); -- { serverError BAD_TYPE_OF_FIELD }

-- A default expression of `locality_hash` in the tags table which contradicts the invariant is rejected:
-- the write paths don't provide that column when inserting into the tags table, so a wrong stored value
-- would make PromQL selectors silently return no data.
CREATE TABLE ts_bad_tags_default ENGINE = TimeSeries TAGS INNER COLUMNS (locality_hash UInt64 DEFAULT 0); -- { serverError INCORRECT_QUERY }
CREATE TABLE ts_bad_tags_expr ENGINE = TimeSeries TAGS INNER COLUMNS (locality_hash UInt64 MATERIALIZED sipHash64(metric_name)); -- { serverError INCORRECT_QUERY }

-- An external tags table must declare the same MATERIALIZED expression.
CREATE TABLE ext_samples (locality_hash UInt64, id UUID, timestamp DateTime64(3), value Float64) ENGINE = MergeTree ORDER BY (locality_hash, id, timestamp);
CREATE TABLE ext_tags_no_default (id UUID, metric_name LowCardinality(String), locality_hash UInt64, tags Map(LowCardinality(String), String), min_time Nullable(DateTime64(3)), max_time Nullable(DateTime64(3))) ENGINE = ReplacingMergeTree ORDER BY (metric_name, id) SETTINGS allow_nullable_key = 1;
CREATE TABLE ext_metrics (metric_family_name String, type String, unit String, help String) ENGINE = ReplacingMergeTree ORDER BY metric_family_name;
CREATE TABLE ts_bad_external ENGINE = TimeSeries SAMPLES ext_samples TAGS ext_tags_no_default METRICS ext_metrics; -- { serverError INCORRECT_QUERY }
DROP TABLE ext_samples;
DROP TABLE ext_tags_no_default;
DROP TABLE ext_metrics;

-- `locality_hash` is a reserved column name for the `tags_to_columns` setting.
CREATE TABLE ts_bad_tag_column ENGINE = TimeSeries SETTINGS tags_to_columns = {'region': 'locality_hash'}; -- { serverError INVALID_SETTING_VALUE }

-- A SummingMergeTree samples engine would sum `locality_hash` during merges unless it is in the sorting key.
CREATE TABLE ts_bad_summing ENGINE = TimeSeries SAMPLES INNER ENGINE = SummingMergeTree ORDER BY (id, timestamp); -- { serverError INCORRECT_QUERY }
CREATE TABLE ts_good_summing ENGINE = TimeSeries SAMPLES INNER ENGINE = SummingMergeTree ORDER BY (locality_hash, id, timestamp);
DROP TABLE ts_good_summing;
SELECT 'validation ok';

DROP TABLE ts_locality;
