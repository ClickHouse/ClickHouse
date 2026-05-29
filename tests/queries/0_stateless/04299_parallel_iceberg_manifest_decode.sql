-- Smoke test for the `iceberg_parallel_manifest_decode_threads` setting added together
-- with parallel iceberg manifest decoding in `IcebergIterator`. End-to-end functional
-- behavior (per-query parallelism, result parity across thread counts) is covered by the
-- integration test `test_storage_iceberg_with_spark/test_parallel_manifest_decode.py`;
-- this stateless test only verifies that the setting is registered, accepts the
-- documented value range, and survives roundtrip via `system.settings`.

-- The setting must exist with the documented type and default.
SELECT name, type, default
FROM system.settings
WHERE name = 'iceberg_parallel_manifest_decode_threads';

-- It must accept a range of values without error. The 0 value is silently bumped
-- to 1 inside `resolveParallelManifestDecodeThreads`; large values are accepted
-- and effectively clamped to the snapshot's manifest count at construction.
SELECT 1 SETTINGS iceberg_parallel_manifest_decode_threads = 0;
SELECT 1 SETTINGS iceberg_parallel_manifest_decode_threads = 1;
SELECT 1 SETTINGS iceberg_parallel_manifest_decode_threads = 8;
SELECT 1 SETTINGS iceberg_parallel_manifest_decode_threads = 64;
SELECT 1 SETTINGS iceberg_parallel_manifest_decode_threads = 1000;

-- Per-query SET roundtrip.
SET iceberg_parallel_manifest_decode_threads = 16;
SELECT getSetting('iceberg_parallel_manifest_decode_threads');
