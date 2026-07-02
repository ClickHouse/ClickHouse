-- Tags: no-fasttest
-- Test: exercises `local_blob_storage` + `plain` metadata combination — the unique configuration
-- enabled by PR #60396. The combination is currently exercised only via integration tests
-- (test_attach_backup_from_s3_plain). This test verifies:
--   1. `MetadataStorageFromPlainObjectStorage` can be instantiated on top of `LocalObjectStorage`
--      via the new disk configuration syntax (`type=object_storage, object_storage_type=local_blob_storage,
--      metadata_type=plain`)
--   2. `system.disks` correctly reports `object_storage_type='Local'`, `metadata_type='Plain'`,
--      `is_write_once=1` for this combination.
--   3. The plain object storage rejects writes with `TABLE_IS_READ_ONLY` (write-once semantics).
-- Covers:
--   src/Disks/DiskType.cpp: `metadataTypeFromString("plain")` (line 18)
--   src/Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.cpp:423
--      `existsOrHasAnyChild` (called by `MetadataStorageFromPlainObjectStorage::existsDirectory`)
--   src/Storages/System/StorageSystemDisks.cpp:91-92 — `object_storage_type`/`metadata_type` columns

DROP TABLE IF EXISTS test_04201_local_plain;

CREATE TABLE test_04201_local_plain (a Int32, b String) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(name='disk_04201_local_plain',
                     type = object_storage,
                     object_storage_type = local_blob_storage,
                     metadata_type = plain,
                     path = './disks/04201_local_plain/');

-- Verify the disk shows up correctly in system.disks with the new columns
SELECT type, object_storage_type, metadata_type, is_write_once
FROM system.disks WHERE name = 'disk_04201_local_plain';

-- Empty table: SELECT must return 0 (exercises `existsDirectory` -> `existsOrHasAnyChild`)
SELECT count() FROM test_04201_local_plain;

-- INSERT must fail with read-only (plain object storage is write-once)
INSERT INTO test_04201_local_plain VALUES (1, 'a'); -- { serverError TABLE_IS_READ_ONLY }

DROP TABLE test_04201_local_plain;
