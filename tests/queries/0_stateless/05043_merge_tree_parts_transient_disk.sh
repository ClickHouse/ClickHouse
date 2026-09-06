#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree, no-random-merge-tree-settings
# no-shared-merge-tree: custom disk
# no-random-merge-tree-settings: the table function is given the granularity of the source table explicitly

# The disk of `mergeTreeParts` is local to the query: it must not appear in `system.disks`, and it
# must not be created (a local disk creates its directory on startup) before the query is authorized.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DISK_ROOT="${CLICKHOUSE_DISKS_FILES}/mtp_transient_${CLICKHOUSE_DATABASE}"
INDEX_GRANULARITY_BYTES=10485760

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mtp_transient_source SYNC;
    CREATE TABLE mtp_transient_source (id Int64)
    ENGINE = MergeTree ORDER BY id
    SETTINGS disk = disk(type = local, path = '${DISK_ROOT}/data/'),
        index_granularity = 512,
        index_granularity_bytes = ${INDEX_GRANULARITY_BYTES},
        min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
    INSERT INTO mtp_transient_source SELECT number FROM numbers(2000);"

PARTS=$(${CLICKHOUSE_CLIENT} --format TSVRaw --query "
    SELECT arrayStringConcat(groupArray(
        part_type || '(path = ''' || replaceOne(path, '${DISK_ROOT}/', '')
        || ''', marks_count = ' || toString(marks)
        || ', ranges = [(0, ' || toString(marks - 1) || ')]'
        || ', has_lightweight_delete = 0)'), ', ')
    FROM system.parts
    WHERE database = currentDatabase() AND table = 'mtp_transient_source' AND active")

function read_parts()
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count(), sum(id) FROM mergeTreeParts(
            structure('id Int64'),
            parts(${PARTS}),
            disk(type = local, path = '${DISK_ROOT}/'),
            table_settings(index_granularity_bytes = ${INDEX_GRANULARITY_BYTES}))
        $1" 2>&1
}

# A successful read does not register the disk of the query anywhere: the only disk under the root
# in `system.disks` is the one of the source table.
read_parts ""
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM system.disks WHERE path LIKE '${DISK_ROOT}%'"

# A query that is rejected by the readonly check does not create the disk: the directory the disk
# would have created on startup does not exist afterwards.
UNAUTHORIZED_ROOT="${DISK_ROOT}/unauthorized/"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM mergeTreeParts(
        structure('id Int64'),
        parts(),
        disk(type = local, path = '${UNAUTHORIZED_ROOT}'),
        table_settings(index_granularity_bytes = ${INDEX_GRANULARITY_BYTES}))
    SETTINGS readonly = 1" 2>&1 | grep -o "READONLY" | head -1
test -d "${UNAUTHORIZED_ROOT}" && echo "the disk was created before the query was authorized" || echo "no directory"

# The kind of source to check access for comes from the `type` of the disk description, so it has
# to be a literal of a known readable type: substitutions, wrappers and named disks are rejected
# before anything is resolved.
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM mergeTreeParts(
        structure('id Int64'), parts(),
        disk(type = cache, path = '${DISK_ROOT}/', disk = 'default'),
        table_settings(index_granularity_bytes = ${INDEX_GRANULARITY_BYTES}))" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM mergeTreeParts(
        structure('id Int64'), parts(),
        disk(path = '${DISK_ROOT}/'),
        table_settings(index_granularity_bytes = ${INDEX_GRANULARITY_BYTES}))" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM mergeTreeParts(
        structure('id Int64'), parts(),
        disk(name = 'mtp_transient_named', type = local, path = '${DISK_ROOT}/'),
        table_settings(index_granularity_bytes = ${INDEX_GRANULARITY_BYTES}))" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

${CLICKHOUSE_CLIENT} --query "DROP TABLE mtp_transient_source SYNC"
