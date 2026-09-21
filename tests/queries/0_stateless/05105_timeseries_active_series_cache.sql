-- Tags: no-fasttest
-- no-fasttest: the selector check below parses PromQL, which needs ANTLR4, disabled in the fast-test build.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_cache;
DROP TABLE IF EXISTS ts_nocache;

-- 1. Table with the active series cache enabled. The cache skips a tags insert, and with it the
-- `min_time` / `max_time` that batch would have contributed, so it needs a table that stores neither.
CREATE TABLE ts_cache ENGINE = TimeSeries
SETTINGS tags_to_columns = {'job': 'job_col', 'instance': 'instance_col'}, store_min_time_and_max_time = 0;

-- First insert: series tags are written to tags table and cached
INSERT INTO ts_cache (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api', 'instance': 'host1:8080'}, [(toDateTime64(1000, 3), 1.0)]);

SELECT 'after first insert (cached):';
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts_cache');
SELECT count() FROM timeSeriesSamples({CLICKHOUSE_DATABASE:String}, 'ts_cache');

-- Second insert with identical series: tag write should be skipped, sample should be written
INSERT INTO ts_cache (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api', 'instance': 'host1:8080'}, [(toDateTime64(1015, 3), 2.0)]);

SELECT 'after second insert (skipped tags):';
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts_cache');
SELECT count() FROM timeSeriesSamples({CLICKHOUSE_DATABASE:String}, 'ts_cache');

-- Third insert with one existing series and one new series:
INSERT INTO ts_cache (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api', 'instance': 'host1:8080'}, [(toDateTime64(1030, 3), 3.0)]),
    ('http_requests', {'job': 'api', 'instance': 'host2:8080'}, [(toDateTime64(1030, 3), 1.0)]);

SELECT 'after mixed insert (one cached, one new):';
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts_cache');
SELECT count() FROM timeSeriesSamples({CLICKHOUSE_DATABASE:String}, 'ts_cache');

-- A skipped series must still be found by a query: the samples are never filtered, and the tags row
-- the first insert wrote is what the selector matches on.
SELECT 'the skipped series is still selectable:';
SELECT count() FROM timeSeriesSelector({CLICKHOUSE_DATABASE:String}, 'ts_cache', 'http_requests{instance="host1:8080"}', toDateTime64(0, 3), toDateTime64(2000, 3));

-- 2. Table with active series cache disabled (tags_cache_max_series = 0)
CREATE TABLE ts_nocache ENGINE = TimeSeries
SETTINGS tags_cache_max_series = 0;

INSERT INTO ts_nocache (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api', 'instance': 'host1:8080'}, [(toDateTime64(1000, 3), 1.0)]);

INSERT INTO ts_nocache (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api', 'instance': 'host1:8080'}, [(toDateTime64(1015, 3), 2.0)]);

SELECT 'cache disabled: both inserts write tags:';
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts_nocache');
SELECT count() FROM timeSeriesSamples({CLICKHOUSE_DATABASE:String}, 'ts_nocache');

-- 3. TRUNCATE clears the active series cache
TRUNCATE TABLE ts_cache;

INSERT INTO ts_cache (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api', 'instance': 'host1:8080'}, [(toDateTime64(2000, 3), 1.0)]);

SELECT 'after truncate and insert:';
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts_cache');
SELECT count() FROM timeSeriesSamples({CLICKHOUSE_DATABASE:String}, 'ts_cache');

-- 4. ALTER TABLE modifies cache setting
ALTER TABLE ts_cache MODIFY SETTING tags_cache_max_series = 0;

INSERT INTO ts_cache (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api', 'instance': 'host1:8080'}, [(toDateTime64(2015, 3), 2.0)]);

SELECT 'after alter to disable cache:';
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts_cache');
SELECT count() FROM timeSeriesSamples({CLICKHOUSE_DATABASE:String}, 'ts_cache');

-- Re-enable cache from 0 via ALTER and set TTL
ALTER TABLE ts_cache MODIFY SETTING tags_cache_max_series = 1000, tags_cache_ttl_seconds = 7200;

INSERT INTO ts_cache (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api', 'instance': 'host1:8080'}, [(toDateTime64(2030, 3), 3.0)]);

SELECT 'after alter to re-enable cache (first insert caches):';
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts_cache');
SELECT count() FROM timeSeriesSamples({CLICKHOUSE_DATABASE:String}, 'ts_cache');

-- Live update of tags_cache_ttl_seconds on active cache
ALTER TABLE ts_cache MODIFY SETTING tags_cache_ttl_seconds = 1800;

INSERT INTO ts_cache (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api', 'instance': 'host1:8080'}, [(toDateTime64(2045, 3), 4.0)]);

SELECT 'after alter ttl on live cache (second insert skips tags):';
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts_cache');
SELECT count() FROM timeSeriesSamples({CLICKHOUSE_DATABASE:String}, 'ts_cache');

-- Reset setting restores defaults (1000000 entries, 1800s TTL)
ALTER TABLE ts_cache RESET SETTING tags_cache_max_series;
ALTER TABLE ts_cache RESET SETTING tags_cache_ttl_seconds;

INSERT INTO ts_cache (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api', 'instance': 'host1:8080'}, [(toDateTime64(2050, 3), 5.0)]);

SELECT 'after reset to defaults (cached series skips tags insert):';
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts_cache');
SELECT count() FROM timeSeriesSamples({CLICKHOUSE_DATABASE:String}, 'ts_cache');

-- 5. A table that stores min_time / max_time gets no cache, whatever tags_cache_max_series says:
-- skipping its tags inserts would freeze the bounds a query prunes series by.
DROP TABLE IF EXISTS ts_bounds;
CREATE TABLE ts_bounds ENGINE = TimeSeries
SETTINGS store_min_time_and_max_time = 1, tags_cache_max_series = 1000;

INSERT INTO ts_bounds (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api', 'instance': 'host1:8080'}, [(toDateTime64(1000, 3), 1.0)]);

INSERT INTO ts_bounds (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api', 'instance': 'host1:8080'}, [(toDateTime64(1015, 3), 2.0)]);

SELECT 'bounds stored: both inserts write tags:';
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts_bounds');
SELECT count() FROM timeSeriesSamples({CLICKHOUSE_DATABASE:String}, 'ts_bounds');

-- and the bounds advance with the samples, which is what the cache would have broken
SELECT 'max_time advanced to the latest sample:';
SELECT max(max_time) = toDateTime64(1015, 3) FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts_bounds');

-- 6. External tags target gets no cache even with store_min_time_and_max_time = 0
DROP TABLE IF EXISTS ext_tags;
DROP TABLE IF EXISTS ts_ext;
CREATE TABLE ext_tags
(
    id Tuple(UInt64, LowCardinality(UUID)),
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String)
)
ENGINE = MergeTree ORDER BY (metric_name, id);

CREATE TABLE ts_ext ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0, store_min_time_and_max_time = 0, tags_cache_max_series = 1000 TAGS ext_tags;

INSERT INTO ts_ext (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api', 'instance': 'host1:8080'}, [(toDateTime64(1000, 3), 1.0)]);

INSERT INTO ts_ext (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api', 'instance': 'host1:8080'}, [(toDateTime64(1015, 3), 2.0)]);

SELECT 'external tags: both inserts write tags:';
SELECT count() FROM ext_tags;
SELECT count() FROM timeSeriesSamples({CLICKHOUSE_DATABASE:String}, 'ts_ext');

DROP TABLE ts_ext;
DROP TABLE ext_tags;

DROP TABLE ts_cache;
DROP TABLE ts_nocache;
DROP TABLE ts_bounds;
