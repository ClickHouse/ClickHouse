-- Tags: no-parallel
-- no-parallel: the style check requires the tag for tests with SYSTEM DROP.

-- The native GCS schema cache must be addressable by name, so that a stale inferred schema
-- can be dropped without restarting the server.
SYSTEM DROP SCHEMA CACHE FOR GCS;

SELECT 'ok';
