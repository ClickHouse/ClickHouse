-- The retention example from the docs of the TimeSeries engine: a TTL on the samples table deletes old samples, the tags stay.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries
SETTINGS recent_samples_partition_by = 'toStartOfInterval(toDateTime(timestamp, ''UTC''), toIntervalHour(5))'
SAMPLES INNER ENGINE = MergeTree PARTITION BY toDate(timestamp, 'UTC') ORDER BY (id, timestamp)
    TTL toDateTime(timestamp, 'UTC') + INTERVAL 30 DAY SETTINGS ttl_only_drop_parts = 1;

SELECT splitByChar('.', name)[3] AS target, partition_key
FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.%samples.%' ORDER BY target;
SELECT splitByChar('.', name)[3] AS target, extract(engine_full, 'TTL (.*) SETTINGS') AS ttl
FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.%samples.%' ORDER BY target;

INSERT INTO ts (metric_name, tags, samples) VALUES
    ('old_metric', {'job': 'test'}, [(now64(3) - INTERVAL 60 DAY, 1.0)]),
    ('new_metric', {'job': 'test'}, [(now64(3) - INTERVAL 1 HOUR, 2.0)]);

OPTIMIZE TABLE ts FINAL;

SELECT 'samples', value FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts);
SELECT 'tags', metric_name FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) ORDER BY metric_name;

DROP TABLE ts;

-- The same recipe with UInt32 timestamps.
CREATE TABLE ts (samples Array(Tuple(UInt32, Float64))) ENGINE = TimeSeries
SETTINGS recent_samples_partition_by = 'toStartOfInterval(toDateTime(timestamp, ''UTC''), toIntervalHour(5))'
SAMPLES INNER ENGINE = MergeTree PARTITION BY toDate(timestamp, 'UTC') ORDER BY (id, timestamp)
    TTL toDateTime(timestamp, 'UTC') + INTERVAL 30 DAY SETTINGS ttl_only_drop_parts = 1;

SELECT splitByChar('.', name)[3] AS target, partition_key
FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.%samples.%' ORDER BY target;
SELECT splitByChar('.', name)[3] AS target, extract(engine_full, 'TTL (.*) SETTINGS') AS ttl
FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.%samples.%' ORDER BY target;
SELECT splitByChar('.', table)[3] AS target, type FROM system.columns
WHERE database = currentDatabase() AND table LIKE '.inner_id.%samples.%' AND name = 'timestamp' ORDER BY target;

INSERT INTO ts (metric_name, tags, samples) VALUES
    ('old_metric', {'job': 'test'}, [(toUInt32(now() - INTERVAL 60 DAY), 1.0)]),
    ('new_metric', {'job': 'test'}, [(toUInt32(now() - INTERVAL 1 HOUR), 2.0)]);

OPTIMIZE TABLE ts FINAL;

SELECT 'samples', value FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts);
SELECT 'tags', metric_name FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) ORDER BY metric_name;

DROP TABLE ts;
