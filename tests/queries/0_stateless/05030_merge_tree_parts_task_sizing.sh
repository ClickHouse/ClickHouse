#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree, no-random-merge-tree-settings
# no-shared-merge-tree: custom disk
# no-random-merge-tree-settings: the table function is given the granularity of the source table explicitly

# The concurrent-read thresholds are counted in marks, and a mark is as big as the granularity of the
# source table, which the `mergeTreeParts` table function only knows from `table_settings(...)`.
# With the settings below, task sizing based on the default granularity instead of the supplied one
# would split the read into several streams, so the stream count of the pipeline is what is checked.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DISK_ROOT="${CLICKHOUSE_DISKS_FILES}/mtpts_${CLICKHOUSE_DATABASE}"

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

# The number of `MergeTreeSelect` streams of the read, from `EXPLAIN PIPELINE`.
function stream_count()
{
    local line
    line=$(${CLICKHOUSE_CLIENT} --query "EXPLAIN PIPELINE $1" | grep 'MergeTreeSelect')
    if [[ ${line} =~ ×[[:space:]]*([0-9]+) ]]
    then
        echo "${BASH_REMATCH[1]}"
    else
        echo 1
    fi
}

echo "--- index_granularity of non-adaptive parts is what sizes the tasks"
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mtpts_fixed SYNC;
    CREATE TABLE mtpts_fixed (dt Date, id Int64, data String)
    ENGINE = MergeTree ORDER BY (dt, id)
    SETTINGS disk = disk(type = local, path = '${DISK_ROOT}/data/'),
        index_granularity = 512,
        index_granularity_bytes = 0,
        min_rows_for_wide_part = 0,
        min_bytes_for_wide_part = 0;
    INSERT INTO mtpts_fixed SELECT '2000-01-01', number, toString(number) FROM numbers(5000);
    INSERT INTO mtpts_fixed SELECT '2000-01-02', number + 5000, toString(number) FROM numbers(5000);
    SYSTEM STOP MERGES mtpts_fixed;"

# The two parts have 10 granules of 512 rows each, 20 marks in total. One concurrent-read task is
# 16384 rows, which is 32 of these marks, so the whole read fits in a single stream. Sized from the
# default granularity of 8192 instead, a task would be 2 marks and the read would fan out.
FIXED_READ="SELECT count() FROM mergeTreeParts(
    structure('dt Date, id Int64, data String'),
    parts($(parts_description mtpts_fixed 0)),
    disk(type = local, path = '${DISK_ROOT}/'),
    table_settings(index_granularity_bytes = 0, index_granularity = 512))
    SETTINGS max_threads = 16,
        merge_tree_min_rows_for_concurrent_read_for_remote_filesystem = 16384,
        merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem = 1,
        merge_tree_min_read_task_size = 1"

stream_count "${FIXED_READ}"
${CLICKHOUSE_CLIENT} --query "${FIXED_READ}"

echo "--- index_granularity_bytes is what sizes the tasks of a byte-based threshold"
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mtpts_adaptive SYNC;
    CREATE TABLE mtpts_adaptive (dt Date, id Int64, data String)
    ENGINE = MergeTree ORDER BY (dt, id)
    SETTINGS disk = disk(type = local, path = '${DISK_ROOT}/data/'),
        index_granularity = 512,
        index_granularity_bytes = 1048576,
        min_rows_for_wide_part = 0,
        min_bytes_for_wide_part = 0;
    INSERT INTO mtpts_adaptive SELECT '2000-01-01', number, toString(number) FROM numbers(5000);
    INSERT INTO mtpts_adaptive SELECT '2000-01-02', number + 5000, toString(number) FROM numbers(5000);
    SYSTEM STOP MERGES mtpts_adaptive;"

# One concurrent-read task is 32 MiB, which is 32 marks of the supplied 1 MiB `index_granularity_bytes`,
# so again a single stream. Sized from the default of 10 MiB instead, a task would be 4 marks.
ADAPTIVE_READ="SELECT count() FROM mergeTreeParts(
    structure('dt Date, id Int64, data String'),
    parts($(parts_description mtpts_adaptive 1)),
    disk(type = local, path = '${DISK_ROOT}/'),
    table_settings(index_granularity_bytes = 1048576, index_granularity = 512))
    SETTINGS max_threads = 16,
        merge_tree_min_rows_for_concurrent_read_for_remote_filesystem = 1,
        merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem = 33554432,
        merge_tree_min_read_task_size = 1"

stream_count "${ADAPTIVE_READ}"
${CLICKHOUSE_CLIENT} --query "${ADAPTIVE_READ}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE mtpts_fixed SYNC; DROP TABLE mtpts_adaptive SYNC"
