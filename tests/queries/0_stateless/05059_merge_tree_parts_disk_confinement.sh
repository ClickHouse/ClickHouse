#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree
# no-shared-merge-tree: custom disk

# The disk of `mergeTreeParts` must not touch anything outside `custom_local_disks_base_directory`.
# Creating a disk starts it, and a local disk creates its directory right there (a local object
# storage even in its constructor), so a description pointing outside has to be rejected before the
# disk is created - not after, when the directory already exists.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

INDEX_GRANULARITY_BYTES=10485760
OUTSIDE_ROOT="$(dirname "${CLICKHOUSE_DISKS_FILES}")/mtp_outside_${CLICKHOUSE_DATABASE}/"
INSIDE_ROOT="${CLICKHOUSE_DISKS_FILES}/mtp_confined_${CLICKHOUSE_DATABASE}/"

# Prints the count for an accepted description, or the error code for a rejected one.
function read_empty()
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM mergeTreeParts(
            structure('id Int64'),
            parts(),
            disk($1),
            table_settings(index_granularity_bytes = ${INDEX_GRANULARITY_BYTES}))" 2>&1 | grep -o "BAD_ARGUMENTS\|^[0-9]\+$" | head -1
}

function check_outside()
{
    test -d "${OUTSIDE_ROOT}" && echo "a directory was created outside the base directory" || echo "no directory"
}

echo "-- a local disk outside the base directory"
read_empty "type = local, path = '${OUTSIDE_ROOT}'"
check_outside

echo "-- a local disk that leaves the base directory through .."
read_empty "type = local, path = '${CLICKHOUSE_DISKS_FILES}/../mtp_outside_${CLICKHOUSE_DATABASE}/'"
check_outside

echo "-- a local object storage outside the base directory"
read_empty "type = object_storage, object_storage_type = local, metadata_type = plain, path = '${OUTSIDE_ROOT}'"
check_outside

echo "-- the compatibility name of a local object storage outside the base directory"
read_empty "type = local_blob_storage, path = '${OUTSIDE_ROOT}'"
check_outside

echo "-- the metadata of an object storage disk outside the base directory"
read_empty "type = object_storage, object_storage_type = local, metadata_type = local, path = '${INSIDE_ROOT}', metadata_path = '${OUTSIDE_ROOT}'"
check_outside

echo "-- inside the base directory, every local backend the factories register is readable"
read_empty "type = local, path = '${INSIDE_ROOT}local/'"
read_empty "type = local_blob_storage, path = '${INSIDE_ROOT}blob/'"
read_empty "type = object_storage, object_storage_type = local, metadata_type = local, path = '${INSIDE_ROOT}object/', metadata_path = '${INSIDE_ROOT}metadata/'"
read_empty "type = object_storage, object_storage_type = local_plain, path = '${INSIDE_ROOT}plain/'"
read_empty "type = object_storage, object_storage_type = local_plain_rewritable, path = '${INSIDE_ROOT}plain_rewritable/'"
