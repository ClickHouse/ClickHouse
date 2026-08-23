#!/usr/bin/env bash
# Tags: no-shared-merge-tree
# Tag no-shared-merge-tree: the metadata-version branch that decides this case is only
# reachable on engines without a metadata version, and the runner rewrites bare MergeTree.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Waits until the table has no unfinished mutation, or until it has failed.
# A synchronous ALTER cannot be used as the oracle: StorageMergeTree::waitForMutation
# returns as soon as a mutation reports any latest_fail_reason, so it can come back
# before the mutation has been applied to every part.
wait_for_mutations() {
    local table=$1
    local i
    for i in {1..300}; do
        if [ "$(${CLICKHOUSE_CLIENT} -q "
                SELECT count() FROM system.mutations
                WHERE database = currentDatabase() AND table = '${table}' AND NOT is_done")" = "0" ]; then
            return 0
        fi
        if [ "$(${CLICKHOUSE_CLIENT} -q "
                SELECT count() FROM system.mutations
                WHERE database = currentDatabase() AND table = '${table}'
                  AND NOT is_done AND latest_fail_reason != ''")" != "0" ]; then
            return 0
        fi
        sleep 0.3
    done
}

# $1 = table name, $2 = extra CREATE settings selecting the part type
run_dropped_column_case() {
    local table=$1
    local part_settings=$2

    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${table} SYNC"
    ${CLICKHOUSE_CLIENT} -q "
        CREATE TABLE ${table} (a UInt8, b Int16, c Float32, d String, h String)
        ENGINE = MergeTree ORDER BY a PARTITION BY a % 2
        SETTINGS ${part_settings}"

    ${CLICKHOUSE_CLIENT} -q "
        INSERT INTO ${table}
        SELECT number, number, number, toString(number), toString(number) FROM numbers(100)"

    # Preconditions. Without both of these the mutation never reaches the code under
    # test and every assertion below would pass vacuously.
    ${CLICKHOUSE_CLIENT} -q "
        SELECT 'part_type', arrayStringConcat(arraySort(groupUniqArray(part_type)), ',')
        FROM system.parts WHERE database = currentDatabase() AND table = '${table}' AND active"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT 'h_has_files_before', count() > 0 FROM system.parts_columns
        WHERE database = currentDatabase() AND table = '${table}' AND active AND column = 'h'"

    # Detaching the partition lets h be dropped from the table while a part that still
    # holds h's files stays on disk. Re-attaching it yields a part whose column set is a
    # strict superset of the table's.
    ${CLICKHOUSE_CLIENT} -q "ALTER TABLE ${table} DETACH PARTITION 0"
    ${CLICKHOUSE_CLIENT} -q "ALTER TABLE ${table} DROP COLUMN h"
    ${CLICKHOUSE_CLIENT} -q "ALTER TABLE ${table} ATTACH PARTITION 0"

    ${CLICKHOUSE_CLIENT} -q "
        SELECT 'h_in_part_not_in_metadata',
               (SELECT count() FROM system.parts_columns
                WHERE database = currentDatabase() AND table = '${table}' AND active AND column = 'h') > 0
           AND (SELECT count() FROM system.columns
                WHERE database = currentDatabase() AND table = '${table}' AND name = 'h') = 0"

    ${CLICKHOUSE_CLIENT} -q "ALTER TABLE ${table} UPDATE b = b + 1 WHERE 1 SETTINGS alter_sync = 0"
    wait_for_mutations "${table}"

    ${CLICKHOUSE_CLIENT} -q "
        SELECT 'unfinished_mutations', count() FROM system.mutations
        WHERE database = currentDatabase() AND table = '${table}' AND NOT is_done"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT 'unknown_identifier_failures', count() FROM system.mutations
        WHERE database = currentDatabase() AND table = '${table}'
          AND latest_fail_error_code_name = 'UNKNOWN_IDENTIFIER'"

    # The read of h was skipped, the rest of the row was not: every row survives and b
    # was incremented exactly once in both partitions.
    ${CLICKHOUSE_CLIENT} -q "
        SELECT 'rows_sum_digest', count(), sum(b), sum(cityHash64(a, c, d)) FROM ${table}"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT 'h_has_files_after', count() FROM system.parts_columns
        WHERE database = currentDatabase() AND table = '${table}' AND active AND column = 'h'"

    ${CLICKHOUSE_CLIENT} -q "DROP TABLE ${table} SYNC"
}

echo 'compact'
run_dropped_column_case t_dropped_column_compact \
    'min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000'

# Wide parts already behaved this way; cover them against a regression.
echo 'wide'
run_dropped_column_case t_dropped_column_wide \
    'min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0'

# MATERIALIZE TTL with ttl_only_drop_parts = 1 reaches the same state through a different
# command. The TTL must not expire here, so the rows survive and the mutation is the only
# thing that can move the assertions below.
echo 'materialize ttl only drop parts'
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_ttl_dropped_column SYNC"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_dropped_column (a UInt8, b Int16, h String, ts DateTime)
    ENGINE = MergeTree ORDER BY a PARTITION BY a % 2
    TTL ts + INTERVAL 30 YEAR
    SETTINGS ttl_only_drop_parts = 1,
             min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000"
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO t_ttl_dropped_column
    SELECT number, number, toString(number), now() FROM numbers(100)"

${CLICKHOUSE_CLIENT} -q "
    SELECT 'ttl_part_type', arrayStringConcat(arraySort(groupUniqArray(part_type)), ',')
    FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_dropped_column' AND active"
${CLICKHOUSE_CLIENT} -q "SELECT 'ttl_rows_survive', count() FROM t_ttl_dropped_column"

${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_ttl_dropped_column DETACH PARTITION 0"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_ttl_dropped_column DROP COLUMN h"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_ttl_dropped_column ATTACH PARTITION 0"

${CLICKHOUSE_CLIENT} -q "
    SELECT 'ttl_h_in_part_not_in_metadata',
           (SELECT count() FROM system.parts_columns
            WHERE database = currentDatabase() AND table = 't_ttl_dropped_column' AND active AND column = 'h') > 0
       AND (SELECT count() FROM system.columns
            WHERE database = currentDatabase() AND table = 't_ttl_dropped_column' AND name = 'h') = 0"

${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_ttl_dropped_column MATERIALIZE TTL SETTINGS alter_sync = 0"
wait_for_mutations t_ttl_dropped_column

${CLICKHOUSE_CLIENT} -q "
    SELECT 'ttl_unfinished_mutations', count() FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_ttl_dropped_column' AND NOT is_done"
${CLICKHOUSE_CLIENT} -q "
    SELECT 'ttl_unknown_identifier_failures', count() FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_ttl_dropped_column'
      AND latest_fail_error_code_name = 'UNKNOWN_IDENTIFIER'"
${CLICKHOUSE_CLIENT} -q "
    SELECT 'ttl_rows_digest', count(), sum(cityHash64(a, b)) FROM t_ttl_dropped_column"
${CLICKHOUSE_CLIENT} -q "
    SELECT 'ttl_h_has_files_after', count() FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_ttl_dropped_column' AND active AND column = 'h'"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_dropped_column SYNC"

# A column still present in the table metadata must keep being rewritten. This fails if
# the skip is over-broad and silently turns legitimate reads into no-ops.
echo 'column still in metadata'
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_present_column SYNC"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_present_column (a UInt8, b Int16) ENGINE = MergeTree ORDER BY a PARTITION BY a % 2
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_present_column SELECT number, number FROM numbers(100)"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_present_column MODIFY COLUMN b Int64 SETTINGS alter_sync = 0"
wait_for_mutations t_present_column
${CLICKHOUSE_CLIENT} -q "
    SELECT 'unfinished_mutations', count() FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_present_column' AND NOT is_done"
${CLICKHOUSE_CLIENT} -q "
    SELECT 'b_type', arrayStringConcat(arraySort(groupUniqArray(type)), ',')
    FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_present_column' AND active AND column = 'b'"
${CLICKHOUSE_CLIENT} -q "SELECT 'rows_sum', count(), sum(b) FROM t_present_column"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_present_column SYNC"
