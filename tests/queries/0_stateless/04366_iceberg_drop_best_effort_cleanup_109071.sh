#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

# Regression test for the self-managed rollback-contract gap in PR #109071.
# DatabaseMemory / DatabaseOnDisk treat table->drop() as transactional and reattach the
# table on any thrown exception. IcebergMetadata::drop with iceberg_delete_data_on_drop=1
# irreversibly deletes files, so if a failure occurs after deletion has started, propagating
# it would roll a partially deleted (corrupted) table back into service. On the self-managed
# path (no catalog) the cleanup must therefore be best-effort: post-delete failures are logged
# and swallowed, and the DROP completes. We force a mid-drop failure with the
# iceberg_drop_catalog_remove_fail failpoint, which fires at the commit point after the data
# files were already deleted.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="db_mem_${CLICKHOUSE_DATABASE}"
TABLE_PATH="${USER_FILES_PATH}/t_04366_${CLICKHOUSE_DATABASE}_${RANDOM}/"
rm -rf "${TABLE_PATH}" 2>/dev/null

# Memory database: dropTable reattaches the table if table->drop() throws.
${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB} ENGINE = Memory"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${DB}.t (x Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}')
"
# Two inserts -> multiple data files, so deletion is under way before the commit point fails.
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${DB}.t SETTINGS allow_insert_into_iceberg = 1 VALUES (1)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${DB}.t SETTINGS allow_insert_into_iceberg = 1 VALUES (2)"

# Fail at the commit point (after data files are deleted). Without the best-effort fix this
# throws out of drop(), DatabaseMemory reattaches the table, and the DROP reports failure while
# the data files are already gone. With the fix the failure is swallowed and the DROP succeeds.
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT iceberg_drop_catalog_remove_fail"

if ${CLICKHOUSE_CLIENT} --iceberg_delete_data_on_drop=1 --query "DROP TABLE ${DB}.t SYNC" 2>/dev/null; then
    echo "drop with mid-cleanup failure: succeeded (best-effort)"
else
    echo "drop with mid-cleanup failure: FAILED (propagated)"
fi

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT iceberg_drop_catalog_remove_fail"

# The table must be gone, not reattached into service.
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${DB}.t" | sed 's/^0$/table dropped/; s/^1$/table STILL EXISTS/'

# Second scenario: a failure in the POST-COMMIT metadata anchor phase must also be swallowed on the
# reattaching path. The anchor is deleted after the commit point, so the table is already gone;
# propagating here would reattach a corrupted (data-deleted) table. iceberg_drop_metadata_anchor_fail
# fires while deleting *.metadata.json.
TABLE_PATH2="${USER_FILES_PATH}/t_04366_anchor_${CLICKHOUSE_DATABASE}_${RANDOM}/"
rm -rf "${TABLE_PATH2}" 2>/dev/null
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB}.t2 (x Int32) ENGINE = IcebergLocal('${TABLE_PATH2}')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${DB}.t2 SETTINGS allow_insert_into_iceberg = 1 VALUES (1)"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT iceberg_drop_metadata_anchor_fail"

if ${CLICKHOUSE_CLIENT} --iceberg_delete_data_on_drop=1 --query "DROP TABLE ${DB}.t2 SYNC" 2>/dev/null; then
    echo "drop with post-commit anchor failure: succeeded (best-effort)"
else
    echo "drop with post-commit anchor failure: FAILED (propagated)"
fi

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT iceberg_drop_metadata_anchor_fail"

${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${DB}.t2" | sed 's/^0$/table dropped/; s/^1$/table STILL EXISTS/'

# Third scenario: on the reattaching path the best-effort switch must begin only AFTER the first
# irreversible delete succeeds. If the VERY FIRST data-file delete fails, nothing has been deleted
# yet, so the failure must PROPAGATE: DatabaseMemory reattaches the still-intact table and the DROP
# reports failure (swallowing here would falsely report success, drop the table, and leave every
# file behind with no retry path). iceberg_drop_first_data_delete_fail fires on the first data delete.
TABLE_PATH3="${USER_FILES_PATH}/t_04366_firstdel_${CLICKHOUSE_DATABASE}_${RANDOM}/"
rm -rf "${TABLE_PATH3}" 2>/dev/null
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB}.t3 (x Int32) ENGINE = IcebergLocal('${TABLE_PATH3}')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${DB}.t3 SETTINGS allow_insert_into_iceberg = 1 VALUES (1)"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT iceberg_drop_first_data_delete_fail"

if ${CLICKHOUSE_CLIENT} --iceberg_delete_data_on_drop=1 --query "DROP TABLE ${DB}.t3 SYNC" 2>/dev/null; then
    echo "drop with first-delete failure: succeeded (best-effort)"
else
    echo "drop with first-delete failure: FAILED (propagated)"
fi

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT iceberg_drop_first_data_delete_fail"

# The table must be reattached intact (still exists), not reported dropped.
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${DB}.t3" | sed 's/^0$/table dropped/; s/^1$/table reattached intact/'

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}"
rm -rf "${TABLE_PATH}" "${TABLE_PATH2}" "${TABLE_PATH3}" 2>/dev/null
