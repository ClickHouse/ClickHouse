SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_cache;
DROP TABLE IF EXISTS ts_nocache;

-- 1. Table with active series cache enabled (default)
CREATE TABLE ts_cache ENGINE = TimeSeries
SETTINGS tags_to_columns = {'job': 'job_col', 'instance': 'instance_col'};

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

DROP TABLE ts_cache;
DROP TABLE ts_nocache;
