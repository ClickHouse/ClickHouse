-- Tags: no-fasttest
-- no-fasttest: needs the `s3_no_cache` storage policy (MinIO) and `system.blob_storage_log`.

-- `REPLACE PARTITION FROM` and `MOVE PARTITION TO TABLE` clone the source part with hardlinks and then
-- rewrite `invalidated_system_columns.txt` for the clone. On a disk with local metadata the hardlink is
-- the very same metadata file, so a rewrite that does not unlink it first goes through the shared inode
-- and `WriteMode::Rewrite` retires the blob it replaces - the blob the source part uploaded - without
-- regard to the other link to it. The two parts then share one blob where they must have one each.
--
-- That anomaly is invisible to reads: the shared inode stays self-consistent, both parts resolve to the
-- clone's blob and its contents are identical. What it does leave behind is the retirement itself, so
-- that is what is asserted here: `system.blob_storage_log` must not show the clone's rewrite deleting a
-- blob that was uploaded for the source table. The `uploads` line is a positive control - it fails if
-- the log is empty or the files were never written, instead of letting the check pass vacuously.
--
-- `min_bytes_for_full_part_storage = 0` pins the full part storage, where
-- `invalidated_system_columns.txt` is a file of its own: a packed part keeps it inside `data.packed`
-- and is a different layout, so the randomized threshold must not decide which one runs here.

DROP TABLE IF EXISTS src_05097;
DROP TABLE IF EXISTS mid_05097;
DROP TABLE IF EXISTS dst_05097;

CREATE TABLE src_05097 (p UInt8, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x SETTINGS storage_policy = 's3_no_cache', min_bytes_for_full_part_storage = 0;
CREATE TABLE mid_05097 (p UInt8, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x SETTINGS storage_policy = 's3_no_cache', min_bytes_for_full_part_storage = 0;
CREATE TABLE dst_05097 (p UInt8, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x SETTINGS storage_policy = 's3_no_cache', min_bytes_for_full_part_storage = 0;

INSERT INTO src_05097 VALUES (1, 1), (1, 2);
-- The moved part is written into `mid_05097` together with an `invalidated_system_columns.txt`.
ALTER TABLE src_05097 MOVE PARTITION 1 TO TABLE mid_05097;

INSERT INTO dst_05097 VALUES (1, 100);
-- The clone shares the blobs of the moved part and rewrites its `invalidated_system_columns.txt`.
ALTER TABLE dst_05097 REPLACE PARTITION 1 FROM mid_05097;

-- A retired blob is logged when it is actually deleted from the object storage.
SYSTEM WAIT BLOBS CLEANUP 's3_no_cache';
SYSTEM FLUSH LOGS blob_storage_log;

WITH
    (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'mid_05097') AS mid_uuid,
    (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'dst_05097') AS dst_uuid,
    (SELECT groupArray(remote_path) FROM system.blob_storage_log
        WHERE disk_name = 's3_no_cache' AND event_type = 'Upload'
            AND local_path LIKE concat('%', mid_uuid, '%/invalidated_system_columns.txt')) AS source_blobs
SELECT
    'uploads', (SELECT count() FROM system.blob_storage_log
        WHERE disk_name = 's3_no_cache' AND event_type = 'Upload'
            AND (local_path LIKE concat('%', mid_uuid, '%/invalidated_system_columns.txt')
                OR local_path LIKE concat('%', dst_uuid, '%/invalidated_system_columns.txt'))),
    'source blobs retired by the clone', (SELECT count() FROM system.blob_storage_log
        WHERE disk_name = 's3_no_cache' AND event_type = 'Delete'
            AND local_path LIKE concat('%', dst_uuid, '%/invalidated_system_columns.txt')
            AND has(source_blobs, remote_path));

-- The source part is still readable, from the object storage: the disk has no cache.
DETACH TABLE mid_05097;
ATTACH TABLE mid_05097;
SELECT 'mid', x FROM mid_05097 ORDER BY x;
SELECT 'dst', x FROM dst_05097 ORDER BY x;

DROP TABLE src_05097;
DROP TABLE mid_05097;
DROP TABLE dst_05097;
