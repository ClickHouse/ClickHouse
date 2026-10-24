#!/usr/bin/env bash
# Tags: no-distributed-cache

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `local` object storage removes the directories of a deleted blob that became empty, but it must stop at
# its own root: that directory is created once, when the disk is created, and is not recreated afterwards.

disk_name="${CLICKHOUSE_TEST_UNIQUE_NAME}"
# The `path` of a custom `local` disk must be inside `custom_local_disks_base_directory`, which is
# `CLICKHOUSE_DISKS_FILES` in tests. Do not use `${disk_name}/` here: that may be where the `local` metadata of the disk goes.
disk_root="${CLICKHOUSE_DISKS_FILES%/}/${disk_name}_blobs"
# The `path` of the disk is not in the canonical form, so the keys of the blobs, which are built from it, are not
# either, while the root that the removal of empty directories stops at is canonicalized. A relative `path` has
# the same effect, but it is resolved against the working directory of the server, which the test cannot rely on.
disk_path="${CLICKHOUSE_DISKS_FILES%/}/../$(basename "${CLICKHOUSE_DISKS_FILES%/}")/${disk_name}_blobs/"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test (a Int32) ENGINE = MergeTree ORDER BY a
    SETTINGS disk = disk(
        name = '${disk_name}',
        type = 'object_storage',
        object_storage_type = 'local',
        metadata_type = 'local',
        path = '${disk_path}',
        wait_for_blob_removal = 1);

    INSERT INTO test VALUES (1);
    SELECT * FROM test;
"

if [ -d "${disk_root}" ]; then
    echo "the root of the object storage was created"
else
    echo "the root of the object storage is not at the expected location: ${disk_root}"
fi

# The blobs of the dropped table are removed before the query returns (`wait_for_blob_removal`), so that the root
# of the object storage is empty. `SYSTEM WAIT BLOBS CLEANUP` cannot be used here: the disk is unregistered as
# soon as the last table using it is dropped.
${CLICKHOUSE_CLIENT} -q "DROP TABLE test SYNC"

if [ -d "${disk_root}" ]; then
    echo "the root of the object storage exists"
else
    echo "the root of the object storage was removed"
fi
