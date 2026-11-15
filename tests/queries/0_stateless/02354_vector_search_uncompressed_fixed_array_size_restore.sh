#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: backups are not configured in the fast test.

# The parts of a table with a column with the setting `uncompressed_fixed_array_size` are read at positions computed from
# the setting, so they can be restored only from a backup of a table with the same definition.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

backup_other="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_other')"
backup_same="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_same')"

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS tab_other;
    DROP TABLE IF EXISTS tab;

    -- Rows of 1 and 3 elements in blocks of 8 bytes: the files have the sizes of one block of 2 elements per row.
    CREATE TABLE tab_other (id UInt32, vec Array(Float32) CODEC(NONE) SETTINGS (min_compress_block_size = 8, max_compress_block_size = 8))
    ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
    INSERT INTO tab_other SELECT number, if(number % 2 = 0, [toFloat32(number)], [toFloat32(number), number + 1, number + 2]) FROM numbers(100);

    CREATE TABLE tab (id UInt32, vec Array(Float32) SETTINGS (uncompressed_fixed_array_size = 2))
    ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
    INSERT INTO tab SELECT number, [toFloat32(number), number + 1] FROM numbers(10);
"

$CLICKHOUSE_CLIENT -q "BACKUP TABLE tab_other TO $backup_other FORMAT Null"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE tab TO $backup_same FORMAT Null"

echo "restore from another definition"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE ${CLICKHOUSE_DATABASE}.tab_other AS ${CLICKHOUSE_DATABASE}.tab FROM $backup_other
    SETTINGS allow_different_table_def = 1, allow_non_empty_tables = 1 FORMAT Null" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE ${CLICKHOUSE_DATABASE}.tab_other AS ${CLICKHOUSE_DATABASE}.tab FROM $backup_other
    SETTINGS allow_non_empty_tables = 1 FORMAT Null" 2>&1 | grep -o -m1 "CANNOT_RESTORE_TABLE"

echo "restore from the same definition"
$CLICKHOUSE_CLIENT -q "DROP TABLE tab SYNC"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE ${CLICKHOUSE_DATABASE}.tab FROM $backup_same FORMAT Null"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(length(vec)) FROM tab"
$CLICKHOUSE_CLIENT -q "SELECT id, vec FROM tab ORDER BY sipHash64(id) LIMIT 3 SETTINGS merge_tree_use_one_row_per_block_layout = 1,
    query_plan_optimize_lazy_materialization = 1, use_uncompressed_cache = 0, use_columns_cache = 0"

$CLICKHOUSE_CLIENT -q "DROP TABLE tab_other"
$CLICKHOUSE_CLIENT -q "DROP TABLE tab"
