-- Keep `system.tables` inside the per-test database. Other concurrent tests can temporarily
-- create recursive `Merge` tables whose metadata is intentionally impossible to enumerate.
CREATE TABLE local_table (value UInt8) ENGINE = Memory;

SELECT database FROM system.tables
WHERE database = currentDatabase() AND database LIKE '%' format Null;
SELECT database AS db FROM system.tables
WHERE database = currentDatabase() AND db LIKE '%' format Null;
SELECT CAST(database, 'String') AS db FROM system.tables
WHERE database = currentDatabase() AND db LIKE '%' format Null;
SELECT CAST('a string', 'Nullable(String)') AS str WHERE str LIKE '%' format Null;
SELECT CAST(database, 'Nullable(String)') AS ndb FROM system.tables
WHERE database = currentDatabase() AND ndb LIKE '%' format Null;
SELECT 'all tests passed';
