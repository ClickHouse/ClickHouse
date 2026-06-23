-- Verifies that `join_runtime_filter_size_from_hash_table_stats` grows an otherwise too-small
-- JOIN runtime bloom filter so that it stays active and filters.

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET collect_hash_table_stats_during_joins = 1;
SET join_runtime_bloom_filter_bytes = 256;

DROP TABLE IF EXISTS rf_build;
DROP TABLE IF EXISTS rf_probe;
CREATE TABLE rf_build (k UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE rf_probe (k UInt64) ENGINE = MergeTree ORDER BY tuple();

-- Exceed exact_values_limit (10000) so a real bloom filter is built.
INSERT INTO rf_build SELECT number FROM numbers(50000);
INSERT INTO rf_probe SELECT number + 45000 FROM numbers(50000);

-- Warm up `HashTablesStatistics` so a size hint exists for the next runs.
SELECT count() FROM rf_probe INNER JOIN rf_build USING (k) FORMAT Null;

-- Stats sizing ON: filter is grown to the observed cardinality, stays active.
SELECT count() FROM rf_probe INNER JOIN rf_build USING (k) SETTINGS join_runtime_filter_size_from_hash_table_stats = 1, log_comment = 'rf_on';

-- Stats sizing OFF: filter stays at 256 bytes, saturates and disables itself.
SELECT count() FROM rf_probe INNER JOIN rf_build USING (k) SETTINGS join_runtime_filter_size_from_hash_table_stats = 0, log_comment = 'rf_off';

SYSTEM FLUSH LOGS query_log;

-- ON: filter active, nothing skipped -> 0
SELECT ProfileEvents['RuntimeFilterRowsSkipped'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = 'rf_on' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- OFF: filter disabled, rows skipped -> 1
SELECT ProfileEvents['RuntimeFilterRowsSkipped'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = 'rf_off' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE rf_build;
DROP TABLE rf_probe;
