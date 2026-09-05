-- Tags: no-fasttest, no-replicated-database

-- `borrow_from_cache` object storage loses all data on restart, so it must use the in-memory metadata storage.
-- Any persistent metadata type would leave stale entries pointing to data that no longer exists.
-- This regression test asserts the factory rejects the misconfiguration upfront.

-- First, create a filesystem cache by making a cached disk.
DROP TABLE IF EXISTS tmp_cache_creator;
CREATE TABLE tmp_cache_creator (x UInt64)
ENGINE = MergeTree() ORDER BY x
SETTINGS disk = disk(
    type = cache,
    disk = 'local_disk',
    name = '04141_cache_creator',
    path = '04141_borrow_test_cache/',
    max_size = '100Mi',
    load_metadata_asynchronously = 0
);

-- Reject: borrow_from_cache requires metadata_type = 'memory'.
DROP TABLE IF EXISTS tmp_borrowed;
CREATE TABLE tmp_borrowed (key UInt64)
ENGINE = MergeTree() ORDER BY key
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = 'borrow_from_cache',
    metadata_type = 'local',
    cache_name = '04141_cache_creator',
    name = '04141_borrowed_disk_local'
); -- { serverError INVALID_CONFIG_PARAMETER }

-- Default (no metadata_type specified) is 'memory', which is allowed.
DROP TABLE IF EXISTS tmp_borrowed;
CREATE TABLE tmp_borrowed (key UInt64)
ENGINE = MergeTree() ORDER BY key
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = 'borrow_from_cache',
    cache_name = '04141_cache_creator',
    name = '04141_borrowed_disk_default'
);

DROP TABLE tmp_borrowed;

-- The inverse direction: in-memory metadata is lost on restart, so it must not be combined with a
-- durable object storage; otherwise a restart would orphan the blobs with no metadata path to clean them up.
DROP TABLE IF EXISTS tmp_durable_memory;
CREATE TABLE tmp_durable_memory (key UInt64)
ENGINE = MergeTree() ORDER BY key
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = 'local_blob_storage',
    metadata_type = 'memory',
    path = '04141_durable_memory/',
    name = '04141_durable_memory_disk'
); -- { serverError INVALID_CONFIG_PARAMETER }

DROP TABLE tmp_cache_creator;
