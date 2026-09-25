#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: `clickhouse local` with a persistent path

# A table created with the deprecated syntax `MergeTree(date, ...)` has format version 0, and the names
# of its parts (`20150101_20150120_1_1_0`) have a layout of their own. `mergeTreeParts` parses them
# with `table_settings(format_version = 0)`; with the default format version they are rejected.
#
# A table with the old syntax cannot be put on a custom disk, so it lives in a `clickhouse local`
# of its own, whose path is also the base directory for the disk of `mergeTreeParts`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LOCAL_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${LOCAL_PATH}"
mkdir -p "${LOCAL_PATH}"

function local_query()
{
    ${CLICKHOUSE_LOCAL} --path "${LOCAL_PATH}" --allow_deprecated_syntax_for_merge_tree 1 --query "$1" \
        -- --custom_local_disks_base_directory="${LOCAL_PATH}/"
}

local_query "
    CREATE TABLE mtpos_old (d Date, id Int64) ENGINE = MergeTree(d, id, 512);
    INSERT INTO mtpos_old SELECT toDate('2015-01-01') + number % 20, number FROM numbers(1000);
    INSERT INTO mtpos_old SELECT toDate('2015-02-01') + number % 20, number FROM numbers(1000);
    SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'mtpos_old' AND active ORDER BY name;"

# `system.parts.path` is absolute, the table function wants it relative to the root of its own disk.
# An adaptive part ends with a final mark that holds no rows, so its granules are `marks - 1`.
PARTS=$(local_query "
    SELECT arrayStringConcat(groupArray(
        part_type || '(path = ''' || replaceOne(path, '${LOCAL_PATH}/store/', '')
        || ''', marks_count = ' || toString(marks)
        || ', ranges = [(0, ' || toString(marks - 1) || ')]'
        || ', has_lightweight_delete = 0)'), ', ')
    FROM (SELECT * FROM system.parts WHERE database = currentDatabase() AND table = 'mtpos_old' AND active ORDER BY name)
    FORMAT TSVRaw")

function read_parts()
{
    local_query "
        SELECT count(), sum(id), min(d), max(d), min(_block_number), max(_block_number) FROM mergeTreeParts(
            structure('d Date, id Int64'),
            parts(${PARTS}),
            disk(type = local, path = '${LOCAL_PATH}/store/'),
            table_settings(index_granularity_bytes = 10485760 $1))" 2>&1 | grep -o "BAD_DATA_PART_NAME\|^[0-9].*$" | head -1
}

echo "--- the default format version"
read_parts ""
echo "--- format_version = 0"
read_parts ", format_version = 0"
echo "--- the source table"
local_query "SELECT count(), sum(id), min(d), max(d), min(_block_number), max(_block_number) FROM mtpos_old"

rm -rf "${LOCAL_PATH}"
