-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: `engine_full` of the inner tables is read from `system.tables` on the initiator only.

-- The settings `samples_partition_by`, `samples_index_granularity`, `samples_index_granularity_bytes` and their `recent_samples_*`
-- counterparts define the partition key and the index granularity of the inner samples tables.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;

SELECT '-- the defaults: one partition per month for the samples table, 5-hour partitions for the recent samples table';

CREATE TABLE ts ENGINE = TimeSeries;
SELECT extract(name, '^\.inner_id\.(\w+)\.') AS inner_table, engine_full
FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.%samples.%' ORDER BY inner_table;

INSERT INTO ts (metric_name, tags, time_series) VALUES
    ('m', map(), [(toDateTime64('2026-01-15 10:00:00', 3), 1.), (toDateTime64('2026-02-15 10:00:00', 3), 2.), (toDateTime64('2026-02-20 10:00:00', 3), 3.)]);
SELECT partition, sum(rows) AS rows
FROM system.parts WHERE database = currentDatabase() AND table LIKE '.inner\_id.samples.%' AND active
GROUP BY partition ORDER BY partition;
DROP TABLE ts;

SELECT '-- the settings define the partition keys and the index granularity';

CREATE TABLE ts ENGINE = TimeSeries
SETTINGS samples_partition_by = 'toStartOfWeek(bucket)', samples_index_granularity = 1024, samples_index_granularity_bytes = 4194304,
         recent_samples_partition_by = 'toStartOfHour(bucket)', recent_samples_index_granularity = 8192, recent_samples_index_granularity_bytes = 65536;
SELECT extract(name, '^\.inner_id\.(\w+)\.') AS inner_table, engine_full
FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.%samples.%' ORDER BY inner_table;
DROP TABLE ts;

SELECT '-- the settings override the engine declaration, the declared values are kept if the settings are not set';

CREATE TABLE ts ENGINE = TimeSeries SETTINGS samples_partition_by = 'toStartOfWeek(bucket)', recent_samples_index_granularity_bytes = 131072
SAMPLES INNER ENGINE = AggregatingMergeTree PARTITION BY toDate(bucket) SETTINGS index_granularity_bytes = 2097152
RECENT SAMPLES INNER ENGINE = AggregatingMergeTree PARTITION BY toDate(bucket) SETTINGS index_granularity_bytes = 2097152;
SELECT extract(name, '^\.inner_id\.(\w+)\.') AS inner_table, engine_full
FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.%samples.%' ORDER BY inner_table;
DROP TABLE ts;

SELECT '-- a partition key requires a MergeTree-family engine, the index granularity is just ignored for other engines';

CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0, samples_partition_by = 'toStartOfWeek(bucket)'
SAMPLES INNER ENGINE = Memory; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0, samples_index_granularity_bytes = 131072
SAMPLES INNER ENGINE = Memory;
SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.samples.%';
DROP TABLE ts;
