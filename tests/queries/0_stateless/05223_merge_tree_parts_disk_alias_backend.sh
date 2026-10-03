#!/usr/bin/env bash
# Tags: no-fasttest, no-object-storage, no-replicated-database, no-shared-merge-tree
# no-fasttest: the `s3` compatibility alias of the `object_storage` disk type is registered only in builds with S3 support
# no-shared-merge-tree: custom disk

# `ObjectStorageFactory::create` lets an explicit `object_storage_type` decide the backend, also for a
# compatibility alias such as `type = s3`, so this is a local disk and neither the S3 credential
# restriction nor the source grant of `S3` applies to it - but the confinement of `mergeTreeParts` to
# `custom_local_disks_base_directory` does.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

INDEX_GRANULARITY_BYTES=10485760
OUTSIDE_ROOT="$(dirname "${CLICKHOUSE_DISKS_FILES}")/mtp_alias_outside_${CLICKHOUSE_DATABASE}/"
INSIDE_ROOT="${CLICKHOUSE_DISKS_FILES}/mtp_alias_confined_${CLICKHOUSE_DATABASE}/"

# Prints the count for an accepted description, or the error code for a rejected one.
function read_empty()
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM mergeTreeParts(
            structure('id Int64'),
            parts(),
            disk($1),
            table_settings(index_granularity_bytes = ${INDEX_GRANULARITY_BYTES}))" 2>&1 | grep -o "BAD_ARGUMENTS\|ACCESS_DENIED\|^[0-9]\+$" | head -1
}

echo "-- a compatibility alias whose backend is overridden by an explicit \`object_storage_type\`, inside the base directory"
read_empty "type = s3, object_storage_type = local, metadata_type = plain, path = '${INSIDE_ROOT}alias/'"

echo "-- the same alias pointing outside the base directory"
read_empty "type = s3, object_storage_type = local, metadata_type = plain, path = '${OUTSIDE_ROOT}'"
test -d "${OUTSIDE_ROOT}" && echo "a directory was created outside the base directory" || echo "no directory"
