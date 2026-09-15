-- Tags: no-fasttest, no-replicated-database, no-shared-merge-tree

-- `DiskObjectStorage::supportZeroCopyReplication` is false for `memory` metadata, so a
-- `ReplicatedMergeTree` on a `borrow_from_cache` disk must replicate parts the ordinary way even
-- with `allow_remote_fs_zero_copy_replication = 1`.
--
-- Unlike `04490_borrow_from_cache_replicated_zero_copy`, this test forces a real inter-replica
-- fetch: the borrowed bytes live in node-local cache segments, and `MetadataStorageInMemory` cannot
-- serialize part metadata (`getSerializedMetadata` throws `NOT_IMPLEMENTED`), so if the zero-copy
-- path were selected again the fetch would not produce a usable part on the second replica.

DROP TABLE IF EXISTS zc_cache_creator;
CREATE TABLE zc_cache_creator (x UInt64)
ENGINE = MergeTree() ORDER BY x
SETTINGS disk = disk(
    type = cache,
    disk = 'local_disk',
    name = '05030_cache_creator',
    path = '05030_borrow_zero_copy_cache/',
    max_size = '100Mi',
    load_metadata_asynchronously = 0
);

DROP TABLE IF EXISTS zc_r1;
DROP TABLE IF EXISTS zc_r2;

CREATE TABLE zc_r1 (key UInt64, value String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05030_borrowed_zero_copy', 'r1')
ORDER BY key
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = 'borrow_from_cache',
    metadata_type = 'memory',
    cache_name = '05030_cache_creator',
    name = '05030_borrowed_zero_copy_disk_r1'
),
allow_remote_fs_zero_copy_replication = 1;

CREATE TABLE zc_r2 (key UInt64, value String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05030_borrowed_zero_copy', 'r2')
ORDER BY key
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = 'borrow_from_cache',
    metadata_type = 'memory',
    cache_name = '05030_cache_creator',
    name = '05030_borrowed_zero_copy_disk_r2'
),
allow_remote_fs_zero_copy_replication = 1;

INSERT INTO zc_r1 VALUES (1, 'hello'), (2, 'world'), (3, 'test');
INSERT INTO zc_r1 VALUES (4, 'another'), (5, 'row');

-- The second replica has to fetch both parts from the first one.
SYSTEM SYNC REPLICA zc_r2;

SELECT 'fetched rows', count() FROM zc_r2;
SELECT * FROM zc_r2 ORDER BY key;

-- A merge on the first replica is replicated as a fetch as well.
OPTIMIZE TABLE zc_r1 FINAL;
SYSTEM SYNC REPLICA zc_r2;
SELECT 'rows after merge', count() FROM zc_r2;
SELECT 'active parts on the second replica', count()
FROM system.parts WHERE database = currentDatabase() AND table = 'zc_r2' AND active;

-- The fetching replica does look for a zero-copy-capable disk (`allow_remote_fs_zero_copy_replication`
-- is on), and every disk of the table refuses, which is exactly the fallback this test protects.
SYSTEM FLUSH LOGS text_log;
SELECT 'zero-copy disk selection attempted', count() > 0
FROM system.text_log
WHERE logger_name LIKE currentDatabase() || '.zc_r2 %(Fetcher)'
  AND message LIKE 'Checking disk %';
SELECT 'disks offering zero-copy', count()
FROM system.text_log
WHERE logger_name LIKE currentDatabase() || '.zc_r2 %(Fetcher)'
  AND message LIKE '% supports zero-copy replication';

DROP TABLE zc_r2;
DROP TABLE zc_r1;
DROP TABLE zc_cache_creator;
