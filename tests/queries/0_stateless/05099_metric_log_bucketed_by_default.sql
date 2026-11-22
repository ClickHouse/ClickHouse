-- `system.metric_log` uses the `bucketed` schema unless `schema_type` says otherwise.

SYSTEM FLUSH LOGS metric_log;

-- All the metrics are in a single `Map` column, serialized in a constant number of buckets.
SELECT name, startsWith(type, 'Map(Enum16(') FROM system.columns
WHERE database = 'system' AND table = 'metric_log' AND name = 'metrics';

SELECT
    extract(engine_full, 'map_serialization_version = ''(\\w+)''') AS map_serialization_version,
    extract(engine_full, 'max_buckets_in_map = (\\d+)') AS max_buckets_in_map,
    extract(engine_full, 'map_buckets_strategy = ''(\\w+)''') AS map_buckets_strategy
FROM system.tables WHERE database = 'system' AND name = 'metric_log';

-- Every metric is still addressable under its own name, as an alias over the map,
-- and the alias carries the documentation of the metric.
SELECT
    name,
    type,
    default_kind,
    default_expression = format('metrics[''{}'']', name) AS aliases_the_map,
    comment != '' AS has_comment
FROM system.columns
WHERE database = 'system' AND table = 'metric_log' AND name IN ('ProfileEvent_Query', 'CurrentMetric_Query')
ORDER BY name;

-- Reading a metric under its own name works (the value is not checked, see 00990).
SELECT max(ProfileEvent_Query) >= 0, max(CurrentMetric_Query) >= 0 FROM system.metric_log;
