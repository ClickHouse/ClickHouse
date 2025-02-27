SELECT * FROM system.numbers WHERE number % 2 = 0 LIMIT 100;
SELECT count() = 2 AS assert_exists FROM system.events WHERE name IN ('FilterTransformPassedRows', 'FilterTransformPassedBytes') HAVING assert_exists;
