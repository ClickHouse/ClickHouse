-- Tags: no-fasttest
-- no-fasttest: needs the `s3_no_cache` storage policy (MinIO).

-- `REPLACE PARTITION FROM` and `MOVE PARTITION TO TABLE` clone the source part with hardlinks and then
-- rewrite its `invalidated_system_columns.txt` for the clone. On a disk with local metadata the hardlink
-- is the very same metadata file, so a rewrite that does not unlink it first goes through the shared
-- file: it retires the blob the source part pointed to and leaves both parts referencing the clone's new
-- blob with an inconsistent link count. The clone must get a blob of its own, and the source part must
-- still load from the object storage afterwards. An uncached S3 disk is used so that the reads after the
-- re-attach go to the object storage.

DROP TABLE IF EXISTS src_05097;
DROP TABLE IF EXISTS mid_05097;
DROP TABLE IF EXISTS dst_05097;

CREATE TABLE src_05097 (p UInt8, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x SETTINGS storage_policy = 's3_no_cache';
CREATE TABLE mid_05097 (p UInt8, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x SETTINGS storage_policy = 's3_no_cache';
CREATE TABLE dst_05097 (p UInt8, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x SETTINGS storage_policy = 's3_no_cache';

INSERT INTO src_05097 VALUES (1, 1), (1, 2);
-- The moved part is written into `mid_05097` together with an `invalidated_system_columns.txt`.
ALTER TABLE src_05097 MOVE PARTITION 1 TO TABLE mid_05097;

INSERT INTO dst_05097 VALUES (1, 100);
-- The clone shares the blobs of the moved part and rewrites its `invalidated_system_columns.txt`.
ALTER TABLE dst_05097 REPLACE PARTITION 1 FROM mid_05097;

SYSTEM WAIT BLOBS CLEANUP 's3_no_cache';

-- The clone and the source part must reference two different blobs.
SELECT 'distinct blobs', count(DISTINCT remote_path)
FROM system.remote_data_paths
WHERE disk_name = 's3_no_cache'
    AND local_path LIKE '%/invalidated_system_columns.txt'
    AND (local_path LIKE concat('%', (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'mid_05097'), '%')
        OR local_path LIKE concat('%', (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'dst_05097'), '%'));

-- Reload the source part from the object storage.
DETACH TABLE mid_05097;
ATTACH TABLE mid_05097;

SELECT 'mid', x FROM mid_05097 ORDER BY x;
SELECT 'dst', x FROM dst_05097 ORDER BY x;
SELECT 'detached', count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 'mid_05097';

DROP TABLE src_05097;
DROP TABLE mid_05097;
DROP TABLE dst_05097;
