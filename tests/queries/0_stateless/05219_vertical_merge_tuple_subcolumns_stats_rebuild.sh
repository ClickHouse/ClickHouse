#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings, no-object-storage, no-shared-merge-tree, no-replicated-database, no-parallel-replicas
#
# no-random-merge-tree-settings: pins Vertical activation and the experimental flatten setting.
# no-object-storage / no-shared-merge-tree: reads part files from a local directory.
#
# Implicit `basic` on a flattenable Tuple does not pin flatten when existing part stats
# can be merged by parent name. When this merge must rebuild stats from the gather
# pipeline (lightweight delete), the parent stays one gathering column so the stats
# step can see `t`.

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
    auto_statistics_types = 'basic, uniq_v2'
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

print_stats_files()
{
    local path="$1"
    find "$path" -maxdepth 1 -type f -printf '%f\n' | grep -E 'statistics' | sort
}

echo '=== two INSERTs still flatten under default implicit basic ==='

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_stats_copy;
    CREATE TABLE t_stats_copy
    (
        k UInt64,
        t Tuple(
            c0 UInt8, c1 UInt8, c2 UInt8, c3 UInt8, c4 UInt8,
            c5 UInt8, c6 UInt8, c7 UInt8, c8 UInt8, c9 UInt8, c10 UInt8)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS};

    INSERT INTO t_stats_copy SETTINGS materialize_statistics_on_insert = 1
        VALUES (1, (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    INSERT INTO t_stats_copy SETTINGS materialize_statistics_on_insert = 1
        VALUES (2, (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    OPTIMIZE TABLE t_stats_copy FINAL;
    SELECT name, statistics FROM system.columns WHERE database = currentDatabase() AND table = 't_stats_copy' AND name = 't';
    SELECT count() FROM t_stats_copy;
    CHECK TABLE t_stats_copy SETTINGS check_query_single_value_result = 1;
"
echo 'merge_algorithm'
print_merge_algorithm t_stats_copy
echo 'stats_files'
print_stats_files "$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_stats_copy' AND active")"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_stats_copy;"

echo
echo '=== lightweight delete rebuilds stats and pins flatten ==='

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_stats_lwd;
    CREATE TABLE t_stats_lwd
    (
        k UInt64,
        t Tuple(
            c0 UInt8, c1 UInt8, c2 UInt8, c3 UInt8, c4 UInt8,
            c5 UInt8, c6 UInt8, c7 UInt8, c8 UInt8, c9 UInt8, c10 UInt8)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS};

    INSERT INTO t_stats_lwd SETTINGS materialize_statistics_on_insert = 1
        VALUES (1, (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)), (2, (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    INSERT INTO t_stats_lwd SETTINGS materialize_statistics_on_insert = 1
        VALUES (3, (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)), (4, (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    DELETE FROM t_stats_lwd WHERE k % 2 = 0;
    OPTIMIZE TABLE t_stats_lwd FINAL;
    SELECT name, statistics FROM system.columns WHERE database = currentDatabase() AND table = 't_stats_lwd' AND name = 't';
    SELECT count() FROM t_stats_lwd;
    CHECK TABLE t_stats_lwd SETTINGS check_query_single_value_result = 1;
"
echo 'merge_algorithm'
print_merge_algorithm t_stats_lwd
echo 'stats_files'
print_stats_files "$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_stats_lwd' AND active")"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_stats_lwd;"
