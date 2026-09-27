-- The retention example from the docs of the TimeSeries engine: a TTL on the samples table deletes old samples, the tags stay.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries
SAMPLES INNER ENGINE = MergeTree PARTITION BY toDate(timestamp) ORDER BY (id, timestamp)
    TTL timestamp + INTERVAL 30 DAY SETTINGS ttl_only_drop_parts = 1;

SELECT partition_key FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.samples.%';
SELECT splitByChar('.', name)[3] AS target, extract(engine_full, 'TTL (.*) SETTINGS') AS ttl
FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.%samples.%' ORDER BY target;

INSERT INTO ts (metric_name, tags, samples) VALUES
    ('old_metric', {'job': 'test'}, [(now64(3) - INTERVAL 60 DAY, 1.0)]),
    ('new_metric', {'job': 'test'}, [(now64(3) - INTERVAL 1 HOUR, 2.0)]);

OPTIMIZE TABLE ts FINAL;

SELECT 'samples', value FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts);
SELECT 'tags', metric_name FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) ORDER BY metric_name;

DROP TABLE ts;
