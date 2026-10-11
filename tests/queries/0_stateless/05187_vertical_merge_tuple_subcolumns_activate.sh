#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings, no-object-storage, no-shared-merge-tree, no-replicated-database, no-parallel-replicas
#
# no-random-merge-tree-settings: pins Vertical activation and the experimental flatten setting.
#
# When allow_experimental_vertical_merge_tuple_subcolumns is on,
# flatten runs before the merge algorithm is chosen, and
# enough_ordinary_cols is gathering_columns.size() after flatten.
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

echo '=== 11 top-level Tuple fields activate Vertical ==='

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_act;
    CREATE TABLE t_act
    (
        k UInt64,
        t Tuple(
            c0 UInt8, c1 UInt8, c2 UInt8, c3 UInt8, c4 UInt8,
            c5 UInt8, c6 UInt8, c7 UInt8, c8 UInt8, c9 UInt8, c10 UInt8)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        allow_experimental_vertical_merge_tuple_subcolumns = 1;

    INSERT INTO t_act VALUES (1, (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    INSERT INTO t_act VALUES (2, (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    OPTIMIZE TABLE t_act FINAL;
    SELECT count() FROM t_act;
    CHECK TABLE t_act SETTINGS check_query_single_value_result = 1;
"
echo 'merge_algorithm'
print_merge_algorithm t_act
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_act;"

echo
echo '=== 10 top-level Tuple fields stay Horizontal ==='

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_short;
    CREATE TABLE t_short
    (
        k UInt64,
        t Tuple(
            c0 UInt8, c1 UInt8, c2 UInt8, c3 UInt8, c4 UInt8,
            c5 UInt8, c6 UInt8, c7 UInt8, c8 UInt8, c9 UInt8)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        allow_experimental_vertical_merge_tuple_subcolumns = 1;

    INSERT INTO t_short VALUES (1, (0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    INSERT INTO t_short VALUES (2, (0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    OPTIMIZE TABLE t_short FINAL;
    SELECT count() FROM t_short;
    CHECK TABLE t_short SETTINGS check_query_single_value_result = 1;
"
echo 'merge_algorithm'
print_merge_algorithm t_short
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_short;"

echo
echo '=== setting off does not count Tuple fields ==='

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_off;
    CREATE TABLE t_off
    (
        k UInt64,
        t Tuple(
            c0 UInt8, c1 UInt8, c2 UInt8, c3 UInt8, c4 UInt8,
            c5 UInt8, c6 UInt8, c7 UInt8, c8 UInt8, c9 UInt8, c10 UInt8)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        allow_experimental_vertical_merge_tuple_subcolumns = 0;

    INSERT INTO t_off VALUES (1, (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    INSERT INTO t_off VALUES (2, (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    OPTIMIZE TABLE t_off FINAL;
    SELECT count() FROM t_off;
    CHECK TABLE t_off SETTINGS check_query_single_value_result = 1;
"
echo 'merge_algorithm'
print_merge_algorithm t_off
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_off;"

echo
echo '=== nested flattenable Tuple expands and activates Vertical ==='

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_nested;
    CREATE TABLE t_nested
    (
        k UInt64,
        t Tuple(
            c0 UInt8, c1 UInt8, c2 UInt8, c3 UInt8, c4 UInt8,
            c5 UInt8, c6 UInt8, c7 UInt8, c8 UInt8, c9 UInt8,
            inner Tuple(x UInt8, y UInt8))
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        allow_experimental_vertical_merge_tuple_subcolumns = 1;

    INSERT INTO t_nested VALUES (1, (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, (1, 2)));
    INSERT INTO t_nested VALUES (2, (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, (3, 4)));
    OPTIMIZE TABLE t_nested FINAL;
    SELECT count() FROM t_nested;
    CHECK TABLE t_nested SETTINGS check_query_single_value_result = 1;
"
echo 'merge_algorithm'
print_merge_algorithm t_nested
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_nested;"

echo
echo '=== JSON sibling does not block flattening the other Tuple ==='

${CLICKHOUSE_CLIENT} -q "
    SET enable_json_type = 1;
    DROP TABLE IF EXISTS t_json;
    CREATE TABLE t_json
    (
        k UInt64,
        t Tuple(
            c0 UInt8, c1 UInt8, c2 UInt8, c3 UInt8, c4 UInt8,
            c5 UInt8, c6 UInt8, c7 UInt8, c8 UInt8, c9 UInt8, c10 UInt8),
        j Tuple(x String, y JSON)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        allow_experimental_vertical_merge_tuple_subcolumns = 1;

    INSERT INTO t_json VALUES (1, (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10), ('a', '{\"p\":1}'));
    INSERT INTO t_json VALUES (2, (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10), ('b', '{\"p\":2}'));
    OPTIMIZE TABLE t_json FINAL;
    SELECT count() FROM t_json;
    CHECK TABLE t_json SETTINGS check_query_single_value_result = 1;
"
echo 'merge_algorithm'
print_merge_algorithm t_json
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_json;"
