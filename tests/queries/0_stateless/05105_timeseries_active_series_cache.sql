-- Tags: no-fasttest
-- no-fasttest: the selector check below parses PromQL, which needs ANTLR4, disabled in the fast-test build.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_cache;
DROP TABLE IF EXISTS ts_nocache;

-- 1. Table with the active series cache enabled. The cache skips a tags insert, and with it the
-- `min_time` / `max_time` that batch would have contributed, so it needs a table that stores neither.
CREATE TABLE ts_cache ENGINE = TimeSeries
SETTINGS tags_to_columns = {'job': 'job_col', 'instance': 'instance_col'}, store_min_time_and_max_time = 0, tags_cache_max_series = 1000;

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

-- Fourth insert with intra-batch duplicate series (one new series repeated twice in batch):
INSERT INTO ts_cache (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api', 'instance': 'host3:8080'}, [(toDateTime64(1040, 3), 1.0)]),
    ('http_requests', {'job': 'api', 'instance': 'host3:8080'}, [(toDateTime64(1045, 3), 2.0)]);

SELECT 'after intra-batch duplicate insert (only one tag row written):';
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

-- A definition without the new setting must remain uncached after ATTACH, as for tables
-- whose metadata predates the setting.
CREATE TABLE ts_legacy ENGINE = TimeSeries SETTINGS store_min_time_and_max_time = 0;
INSERT INTO ts_legacy (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api'}, [(toDateTime64(1000, 3), 1.0)]);
DETACH TABLE ts_legacy;
ATTACH TABLE ts_legacy;
INSERT INTO ts_legacy (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api'}, [(toDateTime64(1015, 3), 2.0)]);
SELECT 'missing cache setting after attach:';
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts_legacy');
DROP TABLE ts_legacy;

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

-- Reset setting restores defaults (cache disabled, 1800s TTL)
ALTER TABLE ts_cache RESET SETTING tags_cache_max_series;
ALTER TABLE ts_cache RESET SETTING tags_cache_ttl_seconds;

INSERT INTO ts_cache (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api', 'instance': 'host1:8080'}, [(toDateTime64(2050, 3), 5.0)]);

SELECT 'after reset to defaults (tags insert resumes):';
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts_cache');
SELECT count() FROM timeSeriesSamples({CLICKHOUSE_DATABASE:String}, 'ts_cache');

-- 5. The point of the split: from version 7 a table which stores min_time / max_time gets the cache
-- too. The bounds live in their own target and are written on every block, cache hit or not, so
-- skipping the repeat tags write no longer freezes the bounds a query prunes series by.
DROP TABLE IF EXISTS ts_bounds;
CREATE TABLE ts_bounds ENGINE = TimeSeries
SETTINGS store_min_time_and_max_time = 1, tags_cache_max_series = 1000;

INSERT INTO ts_bounds (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api', 'instance': 'host1:8080'}, [(toDateTime64(1000, 3), 1.0)]);

INSERT INTO ts_bounds (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api', 'instance': 'host1:8080'}, [(toDateTime64(1015, 3), 2.0)]);

SELECT 'bounds stored: the cached series skips its second tags insert:';
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts_bounds');
SELECT count() FROM timeSeriesSamples({CLICKHOUSE_DATABASE:String}, 'ts_bounds');

-- and the bounds still advance, because they go to their own target on every block
SELECT 'max_time advanced to the latest sample:';
SELECT max(max_time) = toDateTime64(1015, 3) FROM timeSeriesTagsMinMax({CLICKHOUSE_DATABASE:String}, 'ts_bounds');

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

-- A later block that is a full cache hit must still advance the tags pipeline. The target sink keeps
-- the part it just wrote delayed until its next consume(), so skipping the push would let the samples
-- push commit the previous block's samples while its tags were still delayed. Counted through
-- `part_log` rather than by rows, because the tags target is a ReplacingMergeTree and a background
-- merge could collapse the duplicate id.
DROP TABLE IF EXISTS ts_multiblock;
CREATE TABLE ts_multiblock ENGINE = TimeSeries SETTINGS store_min_time_and_max_time = 0, tags_cache_max_series = 1000;

INSERT INTO ts_multiblock (metric_name, tags, samples)
SELECT 'mb_requests', map('job', 'api', 'instance', 'host1:8080'),
       [(toDateTime64(1000 + number * 10, 3), toFloat64(number))]
FROM numbers(2)
SETTINGS max_block_size = 1, max_insert_block_size = 1,
         min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1,
         max_insert_threads = 1;

SYSTEM FLUSH LOGS part_log;

SELECT 'multi-block insert, second block a full cache hit: tags parts match samples parts:';
SELECT
    (SELECT count() FROM system.part_log WHERE database = currentDatabase() AND event_type = 'NewPart'
        AND table = (SELECT concat('.inner_id.tags.', toString(uuid)) FROM system.tables
                     WHERE database = currentDatabase() AND name = 'ts_multiblock'))
    = (SELECT count() FROM system.part_log WHERE database = currentDatabase() AND event_type = 'NewPart'
        AND table = (SELECT concat('.inner_id.samples.', toString(uuid)) FROM system.tables
                     WHERE database = currentDatabase() AND name = 'ts_multiblock'));

DROP TABLE ts_multiblock;
