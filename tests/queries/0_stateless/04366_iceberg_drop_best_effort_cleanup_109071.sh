#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-parallel: failpoints are server-wide.

# A failed file delete in DROP TABLE of an Iceberg table in a Memory database, which reattaches the table if the drop throws.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="db_mem_${CLICKHOUSE_DATABASE}"
TABLE_PATH="${USER_FILES_PATH}/t_04366_${CLICKHOUSE_DATABASE}_${RANDOM}/"
rm -rf "${TABLE_PATH}"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB} ENGINE = Memory"

function drop_with_failpoint()
{
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB}.t (x Int32) ENGINE = IcebergLocal('${TABLE_PATH}')"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO ${DB}.t SETTINGS allow_insert_into_iceberg = 1 VALUES (1)"
    ${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT $1"
    ${CLICKHOUSE_CLIENT} --send_logs_level=fatal --iceberg_delete_data_on_drop=1 --query "DROP TABLE ${DB}.t SYNC" 2>&1 | grep -o -m1 "FAULT_INJECTED"
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT $1"
    ${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${DB}.t"
}

# Failure after the data files are deleted: ignored, the table is dropped.
drop_with_failpoint iceberg_drop_catalog_remove_fail
ls -A "${TABLE_PATH}" 2>/dev/null | wc -l
rm -rf "${TABLE_PATH}"

# Failure on the first delete, nothing is deleted yet: the DROP fails and the table stays readable.
drop_with_failpoint iceberg_drop_first_data_delete_fail
${CLICKHOUSE_CLIENT} --query "SELECT x FROM ${DB}.t"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB}"
rm -rf "${TABLE_PATH}"
