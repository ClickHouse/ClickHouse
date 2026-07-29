-- The native GCS schema cache must be addressable by name, so that a stale inferred schema
-- can be dropped without restarting the server.
SYSTEM DROP SCHEMA CACHE FOR GCS;

SELECT 'ok';
