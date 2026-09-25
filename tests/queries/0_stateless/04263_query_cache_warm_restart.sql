-- Tags: no-parallel
-- Tag no-parallel: query cache is process-global

SYSTEM DROP QUERY CACHE;

-- Verify server setting query_cache_snapshot_enabled exists (should not throw)
SELECT name FROM system.server_settings WHERE name = 'query_cache.snapshot_enabled';

-- Populate cache and verify it works normally when snapshot setting is configured
SELECT 1 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0;

SELECT count() > 0 FROM system.query_cache;

SYSTEM DROP QUERY CACHE;
