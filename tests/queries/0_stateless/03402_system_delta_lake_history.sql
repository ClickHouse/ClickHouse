-- Tags: no-fasttest
-- Tag no-fasttest: Depends on Delta Lake support (USE_PARQUET)

-- Test that system.delta_lake_history table exists and has correct schema
SELECT name, type FROM system.columns WHERE database = 'system' AND table = 'delta_lake_history' ORDER BY position;

-- Test that the table is queryable (should return empty result if no Delta Lake tables exist)
SELECT count() FROM system.delta_lake_history;

-- Test that the table appears in system.tables
SELECT name FROM system.tables WHERE database = 'system' AND name = 'delta_lake_history';
