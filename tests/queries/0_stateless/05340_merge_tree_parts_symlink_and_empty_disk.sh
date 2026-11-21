#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree, no-random-merge-tree-settings
# no-shared-merge-tree: custom disk
# no-random-merge-tree-settings: the table function is given the granularity of the source table explicitly

# A part path of `mergeTreeParts` must not lead outside the root of a local disk through a symlink,
# and a query that reads no marks must not create the disk (a local disk creates its directory).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_ROOT="${CLICKHOUSE_DISKS_FILES}/mtp_symlink_${CLICKHOUSE_DATABASE}"
SOURCE_ROOT="${BASE_ROOT}/source"
READER_ROOT="${BASE_ROOT}/reader"
INDEX_GRANULARITY_BYTES=10485760

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mtp_symlink_source SYNC;
    CREATE TABLE mtp_symlink_source (id Int64)
    ENGINE = MergeTree ORDER BY id
    SETTINGS disk = disk(type = local, path = '${SOURCE_ROOT}/data/'),
        index_granularity = 512,
        index_granularity_bytes = ${INDEX_GRANULARITY_BYTES},
        min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
    INSERT INTO mtp_symlink_source SELECT number FROM numbers(2000);"

# The paths of the parts relative to `SOURCE_ROOT`, as a `parts(...)` list under the given prefix.
function parts_list()
{
    ${CLICKHOUSE_CLIENT} --format TSVRaw --query "
        SELECT arrayStringConcat(groupArray(
            part_type || '(path = ''$1' || replaceOne(path, '${SOURCE_ROOT}/', '')
            || ''', marks_count = ' || toString(marks)
            || ', ranges = [(0, ' || toString(marks - 1) || ')]'
            || ', has_lightweight_delete = 0)'), ', ')
        FROM system.parts
        WHERE database = currentDatabase() AND table = 'mtp_symlink_source' AND active"
}

function read_parts()
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count(), sum(id) FROM mergeTreeParts(
            structure('id Int64'),
            parts($1),
            disk(type = local, path = '$2/'),
            table_settings(index_granularity_bytes = ${INDEX_GRANULARITY_BYTES}))" 2>&1 | grep -o "BAD_ARGUMENTS\|^[0-9]\+.*$" | head -1
}

echo "-- the parts read from the disk they are on"
read_parts "$(parts_list '')" "${SOURCE_ROOT}"

echo "-- the parts reached through a symlink that leads outside the root of the disk"
mkdir -p "${READER_ROOT}"
ln -sfn "${SOURCE_ROOT}" "${READER_ROOT}/link"
read_parts "$(parts_list 'link/')" "${READER_ROOT}"

echo "-- a query without marks to read does not create the disk"
EMPTY_ROOT="${BASE_ROOT}/empty"
read_parts "" "${EMPTY_ROOT}"
read_parts "Wide(path = 'nothing/all_1_1_0/', marks_count = 0, ranges = [], has_lightweight_delete = 0)" "${EMPTY_ROOT}"
test -d "${EMPTY_ROOT}" && echo "the disk was created" || echo "no directory"

${CLICKHOUSE_CLIENT} --query "DROP TABLE mtp_symlink_source SYNC"
rm -rf "${BASE_ROOT}"
