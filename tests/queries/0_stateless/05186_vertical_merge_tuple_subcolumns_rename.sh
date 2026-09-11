#!/usr/bin/env bash
# Tags: no-parallel, no-random-merge-tree-settings, no-object-storage, no-shared-merge-tree, no-replicated-database, no-parallel-replicas
# Tag no-parallel: uses the server-global failpoint mt_select_parts_to_mutate_no_free_threads
#
# no-random-merge-tree-settings: pins flatten settings and Vertical activation.
#
# Pending metadata-only `RENAME t → t2`: parts still store `t`, but flattened
# leaf reads must resolve under the new name.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

cleanup() {
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads" 2>/dev/null || true
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_ren_pend" 2>/dev/null || true
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_ren_pend;
    CREATE TABLE t_ren_pend
    (
        k UInt64,
        t Tuple(s String, n UInt8)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS
        min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0,
        min_bytes_for_full_part_storage = 0,
        enable_block_number_column = 0,
        enable_block_offset_column = 0,
        vertical_merge_algorithm_min_rows_to_activate = 1,
        vertical_merge_algorithm_min_columns_to_activate = 1,
        allow_experimental_vertical_merge_tuple_subcolumns = 1,
        vertical_merge_tuple_subcolumns_fat_threshold_bytes = 1;

    INSERT INTO t_ren_pend VALUES (1, ('a', 1));
    INSERT INTO t_ren_pend VALUES (2, ('b', 2));

    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
    SET alter_sync = 0;
    ALTER TABLE t_ren_pend RENAME COLUMN t TO t2;
"

echo 'optimize_pending_rename'
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE t_ren_pend FINAL"

echo 'count'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ren_pend"

echo 'select_t2'
${CLICKHOUSE_CLIENT} -q "SELECT k, t2, t2.s, t2.n FROM t_ren_pend ORDER BY k"

echo 'check'
${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_ren_pend SETTINGS check_query_single_value_result = 1"

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads"
