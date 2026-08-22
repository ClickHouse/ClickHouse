#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree, no-random-merge-tree-settings
# no-shared-merge-tree: custom disk
# no-random-merge-tree-settings: the table function is given the settings of the source table explicitly

# The two table settings of the source table that the parts themselves do not carry:
# `index_granularity` of a part with non-adaptive marks, and `share_nested_offsets`, which decides the
# names of the offsets streams of a `Nested` column.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DISK_ROOT="${CLICKHOUSE_DISKS_FILES}/mtps_${CLICKHOUSE_DATABASE}"

# `system.parts.path` is absolute, the table function wants it relative to the root of its own disk.
# The second argument is how many marks at the end of a part are not granules: adaptive parts end with
# a final mark that holds no rows, non-adaptive parts do not have one.
function parts_description()
{
    ${CLICKHOUSE_CLIENT} --format TSVRaw --query "
        SELECT arrayStringConcat(groupArray(
            part_type || '(path = ''' || replaceOne(path, '${DISK_ROOT}/', '')
            || ''', marks_count = ' || toString(marks)
            || ', ranges = [(0, ' || toString(marks - $2) || ')]'
            || ', has_lightweight_delete = 0)'), ', ')
        FROM (SELECT * FROM system.parts
              WHERE database = currentDatabase() AND table = '$1' AND active
              ORDER BY name)"
}

echo "--- non-adaptive wide parts: index_granularity_bytes = 0"
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mtps_fixed SYNC;
    CREATE TABLE mtps_fixed (dt Date, id Int64, data String)
    ENGINE = MergeTree PARTITION BY dt ORDER BY (dt, id)
    SETTINGS disk = disk(type = local, path = '${DISK_ROOT}/data/'),
        index_granularity = 512,
        index_granularity_bytes = 0,
        min_rows_for_wide_part = 0,
        min_bytes_for_wide_part = 0;
    INSERT INTO mtps_fixed SELECT '2000-01-01', number, toString(number) FROM numbers(5000);
    INSERT INTO mtps_fixed SELECT '2000-01-02', number, toString(number) FROM numbers(5000);"

${CLICKHOUSE_CLIENT} --query "
    SELECT count(), sum(id), uniqExact(dt) FROM mergeTreeParts(
        structure('dt Date, id Int64, data String'),
        parts($(parts_description mtps_fixed 0)),
        disk(type = local, path = '${DISK_ROOT}/'),
        table_settings(index_granularity_bytes = 0, index_granularity = 512))"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(id), uniqExact(dt) FROM mtps_fixed"

# One granule of a non-adaptive part is `index_granularity` rows, and the argument is the only place
# that says so: with the default of 8192 the same read would return a different number of rows.
FIRST_PART=$(${CLICKHOUSE_CLIENT} --format TSVRaw --query "
    SELECT replaceOne(path, '${DISK_ROOT}/', '') FROM system.parts
    WHERE database = currentDatabase() AND table = 'mtps_fixed' AND active ORDER BY name LIMIT 1")

${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM mergeTreeParts(
        structure('dt Date, id Int64, data String'),
        parts(Wide(path = '${FIRST_PART}', marks_count = 10, ranges = [(0, 1)], has_lightweight_delete = 0)),
        disk(type = local, path = '${DISK_ROOT}/'),
        table_settings(index_granularity_bytes = 0, index_granularity = 512))"

echo "--- share_nested_offsets = 0"
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mtps_nested SYNC;
    CREATE TABLE mtps_nested (id Int64, n Nested(a Int64, b String))
    ENGINE = MergeTree ORDER BY id
    SETTINGS disk = disk(type = local, path = '${DISK_ROOT}/data/'),
        index_granularity = 512,
        index_granularity_bytes = 10485760,
        share_nested_offsets = 0,
        min_rows_for_wide_part = 0,
        min_bytes_for_wide_part = 0;
    INSERT INTO mtps_nested SELECT number, [number, number + 1], [toString(number), 'x'] FROM numbers(5000);"

${CLICKHOUSE_CLIENT} --query "
    SELECT count(), sum(id), sum(arraySum(n.a)), uniqExact(n.b) FROM mergeTreeParts(
        structure('id Int64, n Nested(a Int64, b String)'),
        parts($(parts_description mtps_nested 1)),
        disk(type = local, path = '${DISK_ROOT}/'),
        table_settings(index_granularity_bytes = 10485760, share_nested_offsets = 0))"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(id), sum(arraySum(n.a)), uniqExact(n.b) FROM mtps_nested"

${CLICKHOUSE_CLIENT} --query "DROP TABLE mtps_fixed SYNC; DROP TABLE mtps_nested SYNC"
