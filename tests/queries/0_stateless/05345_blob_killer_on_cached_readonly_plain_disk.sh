#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: every replica would create the disk over the same absolute path, so dropping the table on one replica removes the blobs of another.
# A cache layer over a `plain_rewritable` disk whose object storage is read-only never receives dead blobs:
# the wrapped storage removes blobs synchronously and cannot write to such an object storage. So
# `BlobKillerThread` of the cache layer must not schedule its task, even though `plain_rewritable` itself
# does not report itself as read-only.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

disk_path="${CLICKHOUSE_DISKS_FILES}/05345_plain_rewritable_${CLICKHOUSE_DATABASE}/"

${CLICKHOUSE_CLIENT} --multiline -q "
CREATE TABLE writer (a Int32) ENGINE = MergeTree ORDER BY a
SETTINGS table_disk = 1, disk = disk(
    name = '05345_writer_${CLICKHOUSE_DATABASE}',
    type = 'object_storage',
    object_storage_type = 'local',
    metadata_type = 'plain_rewritable',
    metadata_snapshot_write_delay_ms = 0, -- the reader sees the snapshot of the state right after every commit
    path = '${disk_path}');

INSERT INTO writer VALUES (1), (2), (3);

CREATE TABLE reader (a Int32) ENGINE = MergeTree ORDER BY a
SETTINGS table_disk = 1, disk = disk(
    name = '05345_cache_${CLICKHOUSE_DATABASE}',
    type = 'cache',
    max_size = '1Mi',
    max_file_segment_size = '256Ki',
    boundary_alignment = '256Ki',
    path = '05345_cache_${CLICKHOUSE_DATABASE}/',
    disk = disk(
        name = '05345_reader_${CLICKHOUSE_DATABASE}',
        type = 'object_storage',
        object_storage_type = 'local',
        metadata_type = 'plain_rewritable',
        readonly = 1,
        path = '${disk_path}'));

SELECT sum(a) FROM reader;

-- Nothing is deferred on such a disk, so waiting for the cleanup returns immediately instead of hanging.
SYSTEM WAIT BLOBS CLEANUP '05345_cache_${CLICKHOUSE_DATABASE}';

SYSTEM FLUSH LOGS text_log;

SELECT 'cache layer, blob killer started', count() FROM system.text_log
WHERE logger_name = '05345_cache_${CLICKHOUSE_DATABASE}::BlobKillerThread' AND message LIKE 'Execution started%';

SELECT 'cache layer, blob killer not needed', count() > 0 FROM system.text_log
WHERE logger_name = '05345_cache_${CLICKHOUSE_DATABASE}::BlobKillerThread' AND message LIKE 'Execution is not needed%';

DROP TABLE reader SYNC;
DROP TABLE writer SYNC;
"
