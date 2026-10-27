#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: every replica would create the disk over the same absolute path, so dropping the table on one replica removes the blobs of another.
# The `plain_rewritable` metadata storage (like `plain` and `web`) removes blobs synchronously inside the
# transaction and never replicates them, so a disk using it must never schedule the background tasks of
# `BlobKillerThread` and `BlobCopierThread`. A disk with the default `local` metadata still runs the killer.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Custom disks stay registered until the server restarts, so each run uses its own disk names to start the
# threads again and write fresh log lines.
LOCAL_DISK="05136_local_metadata_${CLICKHOUSE_DATABASE}"
PLAIN_DISK="05136_plain_rewritable_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --multiline -q "
-- The \`local\` metadata of a disk lives in \`disks/<disk name>/\`, and a \`local\` object storage deletes its
-- own root directory once it becomes empty, so \`path\` must not be \`disks/<disk name>/\` here.
CREATE TABLE t_local_metadata (a Int32) ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(
    name = '${LOCAL_DISK}',
    type = 'object_storage',
    object_storage_type = 'local',
    metadata_type = 'local',
    path = '${CLICKHOUSE_DISKS_FILES}/${LOCAL_DISK}_blobs/');

CREATE TABLE t_plain_rewritable (a Int32) ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(
    name = '${PLAIN_DISK}',
    type = 'object_storage',
    object_storage_type = 'local',
    metadata_type = 'plain_rewritable',
    path = '${CLICKHOUSE_DISKS_FILES}/${PLAIN_DISK}/');

SYSTEM FLUSH LOGS text_log;

SELECT 'local metadata, blob killer started', count() > 0 FROM system.text_log
WHERE logger_name = '${LOCAL_DISK}::BlobKillerThread' AND message LIKE 'Execution started%'
    AND event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE;

-- The copier reports this at startup even when \`data_background_replication\` is not enabled.
SELECT 'local metadata, blob copier not needed', count() > 0 FROM system.text_log
WHERE logger_name = '${LOCAL_DISK}::BlobCopierThread' AND message LIKE 'Execution is not needed%'
    AND event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE;

SELECT 'plain_rewritable, blob killer started', count() FROM system.text_log
WHERE logger_name = '${PLAIN_DISK}::BlobKillerThread' AND message LIKE 'Execution started%'
    AND event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE;

SELECT 'plain_rewritable, blob killer not needed', count() > 0 FROM system.text_log
WHERE logger_name = '${PLAIN_DISK}::BlobKillerThread' AND message LIKE 'Execution is not needed%'
    AND event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE;

SELECT 'plain_rewritable, blob copier not needed', count() > 0 FROM system.text_log
WHERE logger_name = '${PLAIN_DISK}::BlobCopierThread' AND message LIKE 'Execution is not needed%'
    AND event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE;

-- Nothing is deferred on such a disk, so waiting for the cleanup returns immediately instead of hanging.
SYSTEM WAIT BLOBS CLEANUP '${PLAIN_DISK}';

DROP TABLE t_local_metadata;
DROP TABLE t_plain_rewritable;
"
