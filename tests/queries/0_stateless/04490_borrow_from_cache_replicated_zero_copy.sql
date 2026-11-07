-- Tags: no-fasttest, no-replicated-database, no-shared-merge-tree

-- Regression test for the zero-copy replication contract of `borrow_from_cache` / `memory` metadata.
-- `DiskObjectStorage::supportZeroCopyReplication` returns false for `memory` metadata, so a
-- `ReplicatedMergeTree` placed on such a disk with `allow_remote_fs_zero_copy_replication = 1` must
-- fall back to ordinary replication rather than selecting the zero-copy path: the borrowed bytes live
-- in node-local cache segments that other replicas cannot read, and `MetadataStorageInMemory` cannot
-- serialize part metadata for the zero-copy path (`getSerializedMetadata` throws `NOT_IMPLEMENTED`).
-- The disk fails closed (the zero-copy path is silently skipped), so the table must still work
-- end-to-end instead of erroring late.
--
-- The absence of zero-copy lock nodes is not asserted directly here because the zero-copy ZooKeeper
-- root (`remote_fs_zero_copy_zookeeper_path`, default `/clickhouse/zero_copy`) is shared server-wide
-- and other tests running in parallel can create it.

-- First, create a filesystem cache by making a cached disk.
DROP TABLE IF EXISTS tmp_cache_creator;
CREATE TABLE tmp_cache_creator (x UInt64)
ENGINE = MergeTree() ORDER BY x
SETTINGS disk = disk(
    type = cache,
    disk = 'local_disk',
    name = '04490_cache_creator',
    path = '04490_borrow_zero_copy_cache/',
    max_size = '100Mi',
    load_metadata_asynchronously = 0
);

-- A replicated table on the borrowed disk with zero-copy replication explicitly enabled.
DROP TABLE IF EXISTS tmp_borrowed_replicated;
CREATE TABLE tmp_borrowed_replicated (key UInt64, value String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04490_borrowed_zero_copy', 'r1')
ORDER BY key
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = 'borrow_from_cache',
    metadata_type = 'memory',
    cache_name = '04490_cache_creator',
    name = '04490_borrowed_zero_copy_disk'
),
allow_remote_fs_zero_copy_replication = 1;

INSERT INTO tmp_borrowed_replicated VALUES (1, 'hello'), (2, 'world'), (3, 'test');
INSERT INTO tmp_borrowed_replicated VALUES (4, 'another'), (5, 'row');

-- A merge writes a new part the same way; with zero-copy disabled for this disk the part stays local.
OPTIMIZE TABLE tmp_borrowed_replicated FINAL;

SELECT * FROM tmp_borrowed_replicated ORDER BY key;
SELECT count() FROM tmp_borrowed_replicated;

DROP TABLE tmp_borrowed_replicated;
DROP TABLE tmp_cache_creator;
