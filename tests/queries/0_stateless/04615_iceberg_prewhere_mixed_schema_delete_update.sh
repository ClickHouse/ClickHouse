#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# `no-parallel-replicas`: see comment in `04071_iceberg_orc_prewhere_crash.sh`.
# `StorageObjectStorageCluster` (used when `parallel_replicas_for_cluster_engines = 1`,
# default) does not delegate `supportsPrewhere` to its underlying configuration.
#
# Regression test for issue #111664: DELETE / UPDATE on an Iceberg table with
# data files written both BEFORE and AFTER `ALTER TABLE ADD COLUMN` deletes the
# wrong rows (or corrupts the table) under PREWHERE pushdown. The mutation reads
# the `_row_number` virtual column to build a positional-delete file; a
# schema-changed file goes through the stripped-PREWHERE fallback `FilterTransform`,
# which dropped rows without updating `ChunkInfoRowNumbers`, so `_row_number`
# became the compacted post-filter index instead of the physical position.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Builds an Iceberg(Parquet) table with two pre-ADD-COLUMN data files (numbers
# 0..19) and one post-ADD-COLUMN data file (numbers 20..29 carrying `note`).
create_mixed_schema_table() {
    local table="$1"
    local table_path="${USER_FILES_PATH}/${table}/"
    rm -rf "${table_path}"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table}"
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${table} (number Int) ENGINE = IcebergLocal('${table_path}', 'Parquet')"
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${table} SELECT toInt32(number) FROM numbers(10)"
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${table} SELECT toInt32(number + 10) FROM numbers(10)"
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${table} ADD COLUMN note Nullable(String)"
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${table} (number, note) SELECT toInt32(number + 20), toString(number) FROM numbers(10)"
}

TABLE_RN="t_rn_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_DEL="t_del_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_UPD="t_upd_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_DEL2="t_del2_${CLICKHOUSE_DATABASE}_${RANDOM}"

# 1) `_row_number` under PREWHERE must be the physical row position of each
#    surviving row, not the compacted post-filter index. This is the exact value
#    the mutation path records into the positional-delete file.
# `optimize_move_to_prewhere=1` + `query_plan_optimize_prewhere=1` are pinned on the discriminating
# statements below: the bug only fires when the predicate is pushed to PREWHERE (the runner
# randomizes both off, and with either off the pre-fix result is already correct, so the test would
# not exercise the fix). Both must be on to reach the stripped-PREWHERE fallback path.
PREWHERE_SETTINGS="--optimize_move_to_prewhere=1 --query_plan_optimize_prewhere=1"

create_mixed_schema_table "${TABLE_RN}"
echo "--- _row_number under PREWHERE (physical positions) ---"
# Explicit PREWHERE (not WHERE) guarantees the predicate hits the reader-side / fallback path this
# fix targets, regardless of the WHERE->PREWHERE mover. This is the direct probe of the mechanism.
${CLICKHOUSE_CLIENT} ${PREWHERE_SETTINGS} --query "SELECT number, _row_number FROM ${TABLE_RN} PREWHERE number % 3 = 0 ORDER BY number"

# 2) DELETE. Predicate removes 0,3,6,...,27; the surviving 20 rows must be exactly
#    the complement. Before the fix, rows were removed by compacted position from
#    the pre-ADD-COLUMN files, producing wrong survivors.
create_mixed_schema_table "${TABLE_DEL}"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 ${PREWHERE_SETTINGS} --query "DELETE FROM ${TABLE_DEL} WHERE number % 3 = 0"
echo "--- DELETE survivors ---"
${CLICKHOUSE_CLIENT} --query "SELECT arraySort(groupArray(number)) FROM ${TABLE_DEL}"

# 3) UPDATE (mutation). Rewrites `note` for the same predicate. UPDATE writes a
#    positional-delete for the old rows plus new data files; a wrong `_row_number`
#    deletes the wrong old rows, leaving duplicates and missing rows.
create_mixed_schema_table "${TABLE_UPD}"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2 ${PREWHERE_SETTINGS} --query "ALTER TABLE ${TABLE_UPD} UPDATE note = 'UPD' WHERE number % 3 = 0"
echo "--- UPDATE: total rows ---"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE_UPD}"
echo "--- UPDATE: all numbers (each exactly once, no dup/missing) ---"
${CLICKHOUSE_CLIENT} --query "SELECT arraySort(groupArray(number)) FROM ${TABLE_UPD}"
echo "--- UPDATE: rows carrying the new value ---"
${CLICKHOUSE_CLIENT} --query "SELECT arraySort(groupArrayIf(number, note = 'UPD')) FROM ${TABLE_UPD}"

# 4) Two sequential DELETEs. The second DELETE reads through the positional-delete transform of the
#    first (which sets ChunkInfoRowNumbers.applied_filter) AND the fallback PREWHERE, so it exercises
#    composition into an EXISTING mask. Remove multiples of 4, then multiples of 3.
create_mixed_schema_table "${TABLE_DEL2}"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 ${PREWHERE_SETTINGS} --query "DELETE FROM ${TABLE_DEL2} WHERE number % 4 = 0"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 ${PREWHERE_SETTINGS} --query "DELETE FROM ${TABLE_DEL2} WHERE number % 3 = 0"
echo "--- two sequential DELETEs survivors ---"
${CLICKHOUSE_CLIENT} --query "SELECT arraySort(groupArray(number)) FROM ${TABLE_DEL2}"

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_RN}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_DEL}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_UPD}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_DEL2}"
rm -rf "${USER_FILES_PATH}/${TABLE_RN}/" "${USER_FILES_PATH}/${TABLE_DEL}/" "${USER_FILES_PATH}/${TABLE_UPD}/" "${USER_FILES_PATH}/${TABLE_DEL2}/"
