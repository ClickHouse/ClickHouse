-- A cached disk keeps a queue of dead blobs of its own: every commit moves the blobs that the wrapped
-- storage removed into it, so that `BlobKillerThread` drops the stale cache entries for them. So the killer
-- of the cache layer has to run even when the disk it wraps removes its blobs synchronously and needs no
-- killer of its own.

CREATE TABLE t_cached_plain_rewritable (a Int32) ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(
    name = '05161_cache_over_plain_rewritable',
    type = 'cache',
    max_size = '1Mi',
    path = '05161_cache_over_plain_rewritable/',
    disk = disk(
        name = '05161_plain_rewritable_under_cache',
        type = 'object_storage',
        object_storage_type = 'local',
        metadata_type = 'plain_rewritable',
        path = 'disks/05161_plain_rewritable_under_cache/'));

INSERT INTO t_cached_plain_rewritable VALUES (1);
SELECT * FROM t_cached_plain_rewritable;
DROP TABLE t_cached_plain_rewritable SYNC;

-- The queue of the cache layer is drained, and nothing is deferred on the disk under it.
SYSTEM WAIT BLOBS CLEANUP '05161_cache_over_plain_rewritable';

SYSTEM FLUSH LOGS text_log;

-- Both disks are registered under the names given above, so that the loggers below are the right ones.
SELECT 'disks', count() FROM system.disks
WHERE name IN ('05161_cache_over_plain_rewritable', '05161_plain_rewritable_under_cache');

SELECT 'cache layer, blob killer started', count() > 0 FROM system.text_log
WHERE logger_name = '05161_cache_over_plain_rewritable::BlobKillerThread' AND message LIKE 'Execution started%';

SELECT 'disk under the cache, blob killer started', count() FROM system.text_log
WHERE logger_name = '05161_plain_rewritable_under_cache::BlobKillerThread' AND message LIKE 'Execution started%';
