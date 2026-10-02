#!/usr/bin/env bash
# Tags: no-distributed-cache

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `local` object storage removes the directories of a deleted blob that became empty, but it must stop at
# its own root: that directory is created once, when the disk is created, and is not recreated afterwards.

server_path=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.disks WHERE name = 'default'")

disk_name="${CLICKHOUSE_TEST_UNIQUE_NAME}"
# Do not use `disks/${disk_name}/` here: that is where the `local` metadata of the disk goes.
disk_root="${server_path%/}/disks/${disk_name}_blobs"
# The `path` of the disk is not in the canonical form, so the keys of the blobs, which are built from it, are not
# either, while the root that the removal of empty directories stops at is canonicalized. A relative `path` has
# the same effect, but it is resolved against the working directory of the server, which the test cannot rely on.
disk_path="${server_path%/}/disks/../disks/${disk_name}_blobs/"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test (a Int32) ENGINE = MergeTree ORDER BY a
    SETTINGS disk = disk(
        name = '${disk_name}',
        type = 'object_storage',
        object_storage_type = 'local',
        metadata_type = 'local',
        path = '${disk_path}');

    INSERT INTO test VALUES (1);
    SELECT * FROM test;
"

if [ -d "${disk_root}" ]; then
    echo "the root of the object storage was created"
else
    echo "the root of the object storage is not at the expected location: ${disk_root}"
fi

${CLICKHOUSE_CLIENT} -q "DROP TABLE test SYNC"

# Make sure the blobs of the dropped table are gone, so that the root of the object storage is empty.
${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT BLOBS CLEANUP '${disk_name}'"

if [ -d "${disk_root}" ]; then
    echo "the root of the object storage exists"
else
    echo "the root of the object storage was removed"
fi
