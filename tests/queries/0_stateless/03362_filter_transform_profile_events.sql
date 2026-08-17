SELECT * FROM system.numbers WHERE number % 2 = 0 LIMIT 100;
SELECT count() = 2 AS assert_exists FROM system.events WHERE name IN ('FilterTransformPassedRows', 'FilterTransformPassedBytes') HAVING assert_exists;

SELECT * FROM system.numbers WHERE number % 2 = 0 LIMIT 100 FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['FilterTransformPassedRows'] > 0, ProfileEvents['FilterTransformPassedBytes'] > 0 FROM system.query_log WHERE (type = 'QueryFinish') AND (current_database = currentDatabase()) AND (query = 'SELECT * FROM system.numbers WHERE number % 2 = 0 LIMIT 100 FORMAT Null;');
