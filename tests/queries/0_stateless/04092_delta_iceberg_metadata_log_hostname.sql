-- Verify that system.delta_lake_metadata_log and system.iceberg_metadata_log
-- have the hostname column as the first column with type LowCardinality(String).

-- Force table creation: these system log tables are lazily prepared
-- (only on first flush), so they may not exist yet on a fresh server.
SYSTEM FLUSH LOGS delta_lake_metadata_log, iceberg_metadata_log;

SELECT name, type FROM system.columns
WHERE table = 'delta_lake_metadata_log' AND database = 'system' AND name = 'hostname';

SELECT name, type FROM system.columns
WHERE table = 'iceberg_metadata_log' AND database = 'system' AND name = 'hostname';

-- Verify SELECT hostname does not throw UNKNOWN_IDENTIFIER
SELECT hostname FROM system.delta_lake_metadata_log LIMIT 0;
SELECT hostname FROM system.iceberg_metadata_log LIMIT 0;
