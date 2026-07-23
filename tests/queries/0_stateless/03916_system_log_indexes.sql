-- Tags: no-fasttest
-- Verify that system log tables have secondary indexes on time and query ID columns.

SYSTEM FLUSH LOGS query_log;
SYSTEM FLUSH LOGS metric_log;

SELECT database, table, name, type, granularity
FROM system.data_skipping_indices
WHERE database = 'system' AND table = 'query_log'
ORDER BY name;

-- Check that a table without query_id (metric_log) only has minmax indexes.
SELECT database, table, name, type, granularity
FROM system.data_skipping_indices
WHERE database = 'system' AND table = 'metric_log'
ORDER BY name;
