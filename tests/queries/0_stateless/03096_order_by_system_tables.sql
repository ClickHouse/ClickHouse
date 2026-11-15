SYSTEM FLUSH LOGS /* all tables */;

-- Check for system tables which have non-default sorting key
WITH
    ['asynchronous_metric_log', 'asynchronous_insert_log', 'opentelemetry_span_log', 'coverage_log'] AS known_tables,
    'event_date, event_time' as default_sorting_key
SELECT
    'Table ' || name || ' has non-default sorting key: ' || sorting_key
FROM system.tables
WHERE (database = 'system') AND (engine = 'MergeTree') AND name not like 'minio%' AND (NOT arraySum(arrayMap(x -> position(name, x), known_tables))) AND (sorting_key != default_sorting_key);
