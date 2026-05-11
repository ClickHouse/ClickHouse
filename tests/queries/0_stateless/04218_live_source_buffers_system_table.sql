-- Test that system.live_source_buffers table exists and has the expected schema.

SELECT name, type
FROM system.columns
WHERE database = 'system' AND table = 'live_source_buffers'
ORDER BY position;

-- Table should be queryable (may be empty if no live buffers are active).
SELECT count() >= 0 FROM system.live_source_buffers;
