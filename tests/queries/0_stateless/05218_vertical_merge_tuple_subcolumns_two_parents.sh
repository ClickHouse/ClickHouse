#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings, no-object-storage, no-shared-merge-tree, no-replicated-database, no-parallel-replicas
#
# no-random-merge-tree-settings: pins flatten settings and Vertical activation.
#
# Two flattenable Tuple parents in one table are both replaced by leaves before
# chooseMergeAlgorithm. Their leaf counts add toward
# vertical_merge_algorithm_min_columns_to_activate, and Vertical commits each
# parent group separately.
# auto_statistics_types is empty so implicit parent stats do not pin flatten.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

COMMON_SETTINGS="
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 11,
    allow_experimental_vertical_merge_tuple_subcolumns = 1,
    auto_statistics_types = ''
"

print_merge_algorithm()
{
    local table="$1"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS part_log"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT merge_algorithm
        FROM system.part_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
          AND database = currentDatabase() AND table = '${table}' AND event_type = 'MergeParts'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    "
}

print_parent_substreams()
{
    local table="$1"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT column, arrayJoin(substreams)
        FROM system.parts_columns
        WHERE database = currentDatabase() AND table = '${table}' AND active AND column IN ('t', 'u')
        ORDER BY 1, 2
    "
}

echo '=== two Tuples, 6+5 leaves activate Vertical ==='

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_two_v;
    DROP TABLE IF EXISTS t_two_h;
    CREATE TABLE t_two_v
    (
        k UInt64,
        t Tuple(a0 UInt8, a1 UInt8, a2 UInt8, a3 UInt8, a4 UInt8, a5 UInt8),
        u Tuple(b0 UInt8, b1 UInt8, b2 UInt8, b3 UInt8, b4 UInt8)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS};

    CREATE TABLE t_two_h AS t_two_v
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        enable_vertical_merge_algorithm = 0;

    INSERT INTO t_two_v VALUES (1, (0, 1, 2, 3, 4, 5), (10, 11, 12, 13, 14));
    INSERT INTO t_two_v VALUES (2, (6, 7, 8, 9, 10, 11), (15, 16, 17, 18, 19));
    INSERT INTO t_two_h VALUES (1, (0, 1, 2, 3, 4, 5), (10, 11, 12, 13, 14));
    INSERT INTO t_two_h VALUES (2, (6, 7, 8, 9, 10, 11), (15, 16, 17, 18, 19));

    OPTIMIZE TABLE t_two_v FINAL;
    OPTIMIZE TABLE t_two_h FINAL;

    SELECT count() FROM t_two_v;
    SELECT count() = 0 FROM (SELECT * FROM t_two_v EXCEPT SELECT * FROM t_two_h);
    SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = 't_two_v' AND name IN ('t', 'u')
    ORDER BY name;
    CHECK TABLE t_two_v SETTINGS check_query_single_value_result = 1;
"

echo 'vertical_substreams'
print_parent_substreams t_two_v
echo 'horizontal_substreams'
print_parent_substreams t_two_h
echo 'merge_algorithm'
print_merge_algorithm t_two_v
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_two_v; DROP TABLE t_two_h;"

echo
echo '=== two Tuples, 5+5 leaves stay Horizontal ==='

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_two_short;
    CREATE TABLE t_two_short
    (
        k UInt64,
        t Tuple(a0 UInt8, a1 UInt8, a2 UInt8, a3 UInt8, a4 UInt8),
        u Tuple(b0 UInt8, b1 UInt8, b2 UInt8, b3 UInt8, b4 UInt8)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS};

    INSERT INTO t_two_short VALUES (1, (0, 1, 2, 3, 4), (10, 11, 12, 13, 14));
    INSERT INTO t_two_short VALUES (2, (5, 6, 7, 8, 9), (15, 16, 17, 18, 19));
    OPTIMIZE TABLE t_two_short FINAL;
    SELECT count() FROM t_two_short;
    CHECK TABLE t_two_short SETTINGS check_query_single_value_result = 1;
"
echo 'merge_algorithm'
print_merge_algorithm t_two_short
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_two_short;"
