#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree, no-random-merge-tree-settings
# no-shared-merge-tree: custom disk
# no-random-merge-tree-settings: the table function is given the granularity of the source table explicitly

# Read the data parts of a MergeTree table back with the `mergeTreeParts` table function, through a
# second disk that knows nothing about the table except where its parts are.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DISK_ROOT="${CLICKHOUSE_DISKS_FILES}/mtp_${CLICKHOUSE_DATABASE}"
INDEX_GRANULARITY_BYTES=10485760
HUGE=1000000000

# `system.parts.path` is absolute, the table function wants it relative to the root of its own disk.
function parts_description()
{
    ${CLICKHOUSE_CLIENT} --format TSVRaw --query "
        SELECT arrayStringConcat(groupArray(
            part_type || '(path = ''' || replaceOne(path, '${DISK_ROOT}/', '')
            || ''', marks_count = ' || toString(marks)
            || ', ranges = [(0, ' || toString(marks - 1) || ')]'
            || ', has_lightweight_delete = ' || toString(toUInt8(has_lightweight_delete)) || ')'), ', ')
        FROM (SELECT * FROM system.parts
              WHERE database = currentDatabase() AND table = 'mtp_source' AND active
              ORDER BY name)"
}

# Reads with the parts description cached in ${PARTS}: a client run per description is what makes the
# test slow under sanitizers.
function read_parts()
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT $1 FROM mergeTreeParts(
            structure('dt Date, id Int64, data String'),
            parts(${PARTS}),
            disk(type = local, path = '${DISK_ROOT}/'),
            table_settings(index_granularity_bytes = ${INDEX_GRANULARITY_BYTES}))
        $2"
}

# All 4 combinations of Wide/Compact parts and full/packed part storage.
for min_bytes_for_wide_part in 0 ${HUGE}
do
    for min_bytes_for_full_part_storage in 0 ${HUGE}
    do
        ${CLICKHOUSE_CLIENT} --query "
            DROP TABLE IF EXISTS mtp_source SYNC;
            CREATE TABLE mtp_source (dt Date, id Int64, data String)
            ENGINE = MergeTree PARTITION BY dt ORDER BY (dt, id)
            SETTINGS disk = disk(type = local, path = '${DISK_ROOT}/data/'),
                index_granularity = 512,
                index_granularity_bytes = ${INDEX_GRANULARITY_BYTES},
                min_rows_for_wide_part = 0,
                min_rows_for_full_part_storage = 0,
                min_bytes_for_wide_part = ${min_bytes_for_wide_part},
                min_bytes_for_full_part_storage = ${min_bytes_for_full_part_storage};
            INSERT INTO mtp_source SELECT '2000-01-01', number, toString(number) FROM numbers(2000);
            INSERT INTO mtp_source SELECT '2000-01-02', number, toString(number) FROM numbers(2000);"

        echo "--- min_bytes_for_wide_part = ${min_bytes_for_wide_part}, min_bytes_for_full_part_storage = ${min_bytes_for_full_part_storage}"
        ${CLICKHOUSE_CLIENT} --query "
            SELECT DISTINCT part_type, part_storage_type FROM system.parts
            WHERE database = currentDatabase() AND table = 'mtp_source' AND active"

        PARTS=$(parts_description)

        # A full scan returns the same thing as reading the table itself.
        read_parts "count(), sum(id), uniqExact(dt)" ""
        ${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(id), uniqExact(dt) FROM mtp_source"

        # The same with each part's mark ranges split across several threads.
        read_parts "count(), sum(id), uniqExact(dt)" "SETTINGS max_threads = 4,
            merge_tree_min_rows_for_concurrent_read_for_remote_filesystem = 1,
            merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem = 1,
            merge_tree_min_read_task_size = 1"

        # PREWHERE is pushed down to the part readers.
        read_parts "count(), sum(id)" "PREWHERE id % 2 = 0"
        read_parts "data" "PREWHERE id = 42"

        # A materialized lightweight delete mask is applied. The synchronous mutation is the slowest
        # part of the test, so it runs once per part type, not for every storage type.
        if [[ ${min_bytes_for_full_part_storage} = 0 ]]
        then
            ${CLICKHOUSE_CLIENT} --query "
                DELETE FROM mtp_source WHERE id % 2 = 0
                SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 2;
                SYSTEM STOP MERGES mtp_source;"
            PARTS=$(parts_description)
            read_parts "count(), sum(id)" ""
            ${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(id) FROM mtp_source"
        fi
    done
done

echo "--- no parts"
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), sum(id) FROM mergeTreeParts(
        structure('dt Date, id Int64, data String'),
        parts(),
        disk(type = local, path = '${DISK_ROOT}/'),
        table_settings(index_granularity_bytes = ${INDEX_GRANULARITY_BYTES}))"

${CLICKHOUSE_CLIENT} --query "DROP TABLE mtp_source SYNC"
