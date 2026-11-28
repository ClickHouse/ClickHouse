#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree
# no-shared-merge-tree: custom disk

# `DESCRIBE` of `mergeTreeParts` never creates the disk, but it must still reject a disk description
# that every read rejects: a local path outside `custom_local_disks_base_directory`, or a `from_env`
# substitution while `dynamic_disk_allow_from_env` is off.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

OUTSIDE_ROOT="$(dirname "${CLICKHOUSE_DISKS_FILES}")/mtp_describe_outside_${CLICKHOUSE_DATABASE}/"
INSIDE_ROOT="${CLICKHOUSE_DISKS_FILES}/mtp_describe_${CLICKHOUSE_DATABASE}/"

# Prints the structure for an accepted description, or the error code for a rejected one.
function describe()
{
    ${CLICKHOUSE_CLIENT} --query "
        DESCRIBE mergeTreeParts(
            structure('x UInt8'),
            parts(),
            disk($1),
            table_settings(index_granularity_bytes = 10485760))
        FORMAT TSV" 2>&1 | grep -o "BAD_ARGUMENTS\|ACCESS_DENIED\|^x.*" | head -1
}

echo "-- a local disk inside the base directory"
describe "type = local, path = '${INSIDE_ROOT}local/'"

echo "-- a local disk outside the base directory"
describe "type = local, path = '${OUTSIDE_ROOT}'"

echo "-- a local object storage outside the base directory"
describe "type = object_storage, object_storage_type = local, metadata_type = plain, path = '${OUTSIDE_ROOT}'"

echo "-- the metadata of an object storage disk outside the base directory"
describe "type = object_storage, object_storage_type = local, metadata_type = local, path = '${INSIDE_ROOT}object/', metadata_path = '${OUTSIDE_ROOT}'"

echo "-- from_env without dynamic_disk_allow_from_env"
describe "type = local, path = 'from_env CLICKHOUSE_MTP_DESCRIBE_PATH'"

test -d "${OUTSIDE_ROOT}" && echo "a directory was created outside the base directory" || echo "no directory"
