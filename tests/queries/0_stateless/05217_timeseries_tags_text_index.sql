-- Tags: no-fasttest
-- PromQL requires ANTLR4, which is disabled in the fast-test build.

SET allow_experimental_time_series_table = 1;
SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET explain_query_plan_default = 'legacy';
-- The assertions on `TextIndexReadPostings` below count posting lists read by one query, so keep them independent of what earlier queries have already put into the server-wide postings cache.
SET use_text_index_postings_cache = 0;
SET log_queries = 1;
SET log_profile_events = 1;
-- Direct reads from text indexes require a single replica.
SET max_parallel_replicas = 1;

CREATE TABLE ts_text ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0, tags_index_granularity = 1;

SELECT 'default index';
SELECT name, type_full, expr, granularity FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table =
    (SELECT concat('.inner_id.tags.', toString(uuid)) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_text');

INSERT INTO ts_text (metric_name, tags, samples) VALUES
    ('test_metric', {'instance':'1', 'job':'api', 'zone':'west'}, [(1000, 1.)]),
    ('test_metric', {'instance':'2', 'job':'worker', 'zone':'api'}, [(1000, 2.)]),
    ('test_metric', {'instance':'3', 'job':'api', 'zone':'east'}, [(1000, 3.)]),
    ('test_metric', {'instance':'4', 'job':'', 'zone':'west'}, [(1000, 4.)]),
    ('test_metric', {'instance':'5', 'zone':'west'}, [(1000, 5.)]);

SELECT 'direct read from the tags index';
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT id FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts_text') WHERE tags['job'] = 'worker'
)
WHERE explain LIKE '%__text_index_tags_idx_equals%';

SELECT 'PromQL selector uses the index';
-- The tags subquery is evaluated while building the `IN` set and is absent from `EXPLAIN`.
SELECT value FROM timeSeriesSelector(ts_text, 'test_metric{job="worker"}', 999, 1000)
SETTINGS log_comment = 'timeseries_tags_text_index_selector';
SELECT value FROM prometheusQuery(ts_text, 'test_metric{job="worker"}', 1000)
SETTINGS log_comment = 'timeseries_tags_text_index_promql';
SYSTEM FLUSH LOGS query_log;
SELECT count() = 2 AND min(ProfileEvents['TextIndexUsedEmbeddedPostings'] + ProfileEvents['TextIndexReadPostings'] > 0)
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase()
    AND log_comment IN ('timeseries_tags_text_index_selector', 'timeseries_tags_text_index_promql');

SELECT 'exact PromQL match with and without the index';
SELECT value FROM prometheusQuery(ts_text, 'test_metric{job="api"}', 1000) ORDER BY value;
SELECT value FROM prometheusQuery(ts_text, 'test_metric{job="api"}', 1000) ORDER BY value SETTINGS use_skip_indexes = 0;

SELECT 'multiple labels';
SELECT value FROM prometheusQuery(ts_text, 'test_metric{job="api",zone="west"}', 1000);

SELECT 'missing and empty labels';
SELECT value FROM prometheusQuery(ts_text, 'test_metric{job=""}', 1000) ORDER BY value;
SELECT count() FROM prometheusQuery(ts_text, 'test_metric{missing="api"}', 1000);
SELECT value FROM prometheusQuery(ts_text, 'test_metric{job!="api"}', 1000) ORDER BY value;
SELECT value FROM prometheusQuery(ts_text, 'test_metric{job=~"api|worker"}', 1000) ORDER BY value;

SELECT 'copy and reattach';
CREATE TABLE ts_text_copy AS ts_text;
DETACH TABLE ts_text_copy;
ATTACH TABLE ts_text_copy;
SELECT name, type_full FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table =
    (SELECT concat('.inner_id.tags.', toString(uuid)) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_text_copy');
DROP TABLE ts_text_copy;

SELECT 'custom index';
CREATE TABLE ts_text_custom ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
TAGS INNER COLUMNS (INDEX custom_tags tags TYPE text(tokenizer = 'keyValuePairs'));
CREATE TABLE ts_text_copy AS ts_text_custom;
SELECT name, type_full, granularity FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table =
    (SELECT concat('.inner_id.tags.', toString(uuid)) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_text_copy');
DROP TABLE ts_text_copy;
DROP TABLE ts_text_custom;

SELECT 'previous schema version';
CREATE TABLE ts_text_old ENGINE = TimeSeries SETTINGS version = 4, recent_samples_ttl_seconds = 0;
DETACH TABLE ts_text_old;
ATTACH TABLE ts_text_old;
SELECT count() FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table =
    (SELECT concat('.inner_id.tags.', toString(uuid)) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_text_old');
INSERT INTO ts_text_old (metric_name, tags, samples) VALUES ('test_metric', {'job':'api'}, [(1000, 1.)]);
SELECT value FROM prometheusQuery(ts_text_old, 'test_metric{job="api"}', 1000);
DROP TABLE ts_text_old;

SELECT 'non-MergeTree tags target';
CREATE TABLE ts_text_memory AS ts_text ENGINE = TimeSeries TAGS INNER ENGINE = Memory;
INSERT INTO ts_text_memory SELECT * FROM ts_text;
SELECT count() FROM ts_text_memory;
DROP TABLE ts_text_memory;

SELECT 'external tags target';
CREATE TABLE external_tags
(
    id Tuple(UInt64, LowCardinality(UUID)),
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String)
)
ENGINE = MergeTree ORDER BY (metric_name, id);
CREATE TABLE ts_text_external ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0, store_min_time_and_max_time = 0 TAGS external_tags;
INSERT INTO ts_text_external (metric_name, tags, samples) VALUES ('test_metric', {'job':'api'}, [(1000, 1.)]);
SELECT value FROM prometheusQuery(ts_text_external, 'test_metric{job="api"}', 1000);
DROP TABLE ts_text_external;
DROP TABLE external_tags;
DROP TABLE ts_text;
