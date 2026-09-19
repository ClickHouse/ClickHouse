-- The `plain_rewritable` metadata storage (like `plain` and `web`) removes blobs synchronously inside the
-- transaction and never replicates them, so a disk using it must never schedule the background tasks of
-- `BlobKillerThread` and `BlobCopierThread`. A disk with the default `local` metadata still runs the killer.

-- The `local` metadata of a disk lives in `disks/<disk name>/`, and a `local` object storage deletes its
-- own root directory once it becomes empty, so `path` must not be `disks/05136_local_metadata/` here.
CREATE TABLE t_local_metadata (a Int32) ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(
    name = '05136_local_metadata',
    type = 'object_storage',
    object_storage_type = 'local',
    metadata_type = 'local',
    path = 'disks/05136_local_metadata_blobs/');

CREATE TABLE t_plain_rewritable (a Int32) ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(
    name = '05136_plain_rewritable',
    type = 'object_storage',
    object_storage_type = 'local',
    metadata_type = 'plain_rewritable',
    path = 'disks/05136_plain_rewritable/');

SYSTEM FLUSH LOGS text_log;

SELECT 'local metadata, blob killer started', count() > 0 FROM system.text_log
WHERE logger_name = '05136_local_metadata::BlobKillerThread' AND message LIKE 'Execution started%';

SELECT 'plain_rewritable, blob killer started', count() FROM system.text_log
WHERE logger_name = '05136_plain_rewritable::BlobKillerThread' AND message LIKE 'Execution started%';

SELECT 'plain_rewritable, blob killer not needed', count() > 0 FROM system.text_log
WHERE logger_name = '05136_plain_rewritable::BlobKillerThread' AND message LIKE 'Execution is not needed%';

SELECT 'plain_rewritable, blob copier not needed', count() > 0 FROM system.text_log
WHERE logger_name = '05136_plain_rewritable::BlobCopierThread' AND message LIKE 'Execution is not needed%';

-- Nothing is deferred on such a disk, so waiting for the cleanup returns immediately instead of hanging.
SYSTEM WAIT BLOBS CLEANUP '05136_plain_rewritable';

DROP TABLE t_local_metadata;
DROP TABLE t_plain_rewritable;
