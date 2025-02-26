SELECT * FROM system.numbers WHERE number % 2 = 0 LIMIT 100;
SELECT count() = 2 AS assert_exists FROM system.events WHERE name IN ('FilterTransformPassedRows', 'FilterTransformPassedBytes') HAVING assert_exists;

SYSTEM FLUSH LOGS;
SET log_queries = 1;
SELECT ProfileEvents['FilterTransformPassedRows'] AS passed_rows, ProfileEvents['FilterTransformPassedBytes'] AS passed_bytes FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() ORDER BY event_time DESC LIMIT 1;