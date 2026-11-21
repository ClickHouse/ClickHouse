#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree, no-random-merge-tree-settings
# no-shared-merge-tree: custom disk
# no-random-merge-tree-settings: the table function is given the settings of the source table explicitly

# The persistent virtual columns of a part that is read without its table. When the part does not store
# them, `_row_exists` is the constant 1 and `_block_number` is the minimum block of the part, both taken
# from the part name; when the part does store them, the stored values are read.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DISK_ROOT="${CLICKHOUSE_DISKS_FILES}/mtps_${CLICKHOUSE_DATABASE}"

# `system.parts.path` is absolute, the table function wants it relative to the root of its own disk.
# An adaptive part ends with a final mark that holds no rows, so its granules are `marks - 1`.
function parts_description()
{
    ${CLICKHOUSE_CLIENT} --format TSVRaw --query "
        SELECT arrayStringConcat(groupArray(
            part_type || '(path = ''' || replaceOne(path, '${DISK_ROOT}/', '')
            || ''', marks_count = ' || toString(marks)
            || ', ranges = [(0, ' || toString(marks - 1) || ')]'
            || ', has_lightweight_delete = ' || toString($2) || ')'), ', ')
        FROM (SELECT * FROM system.parts
              WHERE database = currentDatabase() AND table = '$1' AND active
              ORDER BY name)"
}

echo "--- neither column is stored in the part"
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mtps_virtuals SYNC;
    CREATE TABLE mtps_virtuals (id Int64) ENGINE = MergeTree ORDER BY id
    SETTINGS disk = disk(type = local, path = '${DISK_ROOT}/data/'),
        index_granularity = 512,
        index_granularity_bytes = 10485760,
        enable_block_number_column = 0,
        min_rows_for_wide_part = 0,
        min_bytes_for_wide_part = 0;
    INSERT INTO mtps_virtuals SELECT number FROM numbers(1000);
    INSERT INTO mtps_virtuals SELECT number FROM numbers(1000);"

# Without the constant fill, `_row_exists` would come back as 0 for every row and `_block_number` as 0.
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), sum(_row_exists), min(_block_number), max(_block_number) FROM mergeTreeParts(
        structure('id Int64'),
        parts($(parts_description mtps_virtuals 0)),
        disk(type = local, path = '${DISK_ROOT}/'),
        table_settings(index_granularity_bytes = 10485760))"
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), sum(_row_exists), min(_block_number), max(_block_number) FROM mtps_virtuals"

echo "--- both columns are stored in the part"
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mtps_virtuals_stored SYNC;
    CREATE TABLE mtps_virtuals_stored (id Int64) ENGINE = MergeTree ORDER BY id
    SETTINGS disk = disk(type = local, path = '${DISK_ROOT}/data/'),
        index_granularity = 512,
        index_granularity_bytes = 10485760,
        enable_block_number_column = 1,
        min_rows_for_wide_part = 0,
        min_bytes_for_wide_part = 0;
    INSERT INTO mtps_virtuals_stored SELECT number FROM numbers(1000);
    INSERT INTO mtps_virtuals_stored SELECT number FROM numbers(1000);
    DELETE FROM mtps_virtuals_stored WHERE id < 100;"

${CLICKHOUSE_CLIENT} --query "
    SELECT count(), sum(_row_exists), min(_block_number), max(_block_number) FROM mergeTreeParts(
        structure('id Int64'),
        parts($(parts_description mtps_virtuals_stored 1)),
        disk(type = local, path = '${DISK_ROOT}/'),
        table_settings(index_granularity_bytes = 10485760))"

${CLICKHOUSE_CLIENT} --query "DROP TABLE mtps_virtuals SYNC; DROP TABLE mtps_virtuals_stored SYNC"
