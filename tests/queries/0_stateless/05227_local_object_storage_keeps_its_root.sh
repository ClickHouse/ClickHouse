#!/usr/bin/env bash
# Tags: no-distributed-cache

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `local` object storage removes the directories of a deleted blob that became empty, but it must stop at
# its own root: that directory is created once, when the disk is created, and is not recreated afterwards.

server_path=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.disks WHERE name = 'default'")

disk_name="${CLICKHOUSE_TEST_UNIQUE_NAME}"
# A relative `path` is resolved against the data directory of the server. Do not use `disks/${disk_name}/`
# here: that is where the `local` metadata of the disk goes.
disk_root="${server_path%/}/disks/${disk_name}_blobs"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test (a Int32) ENGINE = MergeTree ORDER BY a
    SETTINGS disk = disk(
        name = '${disk_name}',
        type = 'object_storage',
        object_storage_type = 'local',
        metadata_type = 'local',
        path = 'disks/${disk_name}_blobs/');

    INSERT INTO test VALUES (1);
    SELECT * FROM test;
    DROP TABLE test SYNC;
"

# Make sure the blobs of the dropped table are gone, so that the root of the object storage is empty.
${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT BLOBS CLEANUP '${disk_name}'"

if [ -d "${disk_root}" ]; then
    echo "the root of the object storage exists"
else
    echo "the root of the object storage was removed"
fi
