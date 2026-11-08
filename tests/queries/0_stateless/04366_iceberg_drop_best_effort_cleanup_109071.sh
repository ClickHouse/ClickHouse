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
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB}.t (x Int32) ENGINE = IcebergLocal('${TABLE_PATH}'${2:+, '$2'})"
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
for format in "" Avro; do
    drop_with_failpoint iceberg_drop_first_data_delete_fail "${format}"
    ${CLICKHOUSE_CLIENT} --query "SELECT x FROM ${DB}.t"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${DB}.t"
    rm -rf "${TABLE_PATH}"
done

# The first delete takes effect and then throws: ignored, the table is dropped.
drop_with_failpoint local_object_storage_network_error_during_remove
ls -A "${TABLE_PATH}" 2>/dev/null | wc -l
rm -rf "${TABLE_PATH}"

# No data files, so the first file that exists is a metadata file: its failed delete fails the DROP.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB}.t (x Int32) ENGINE = IcebergLocal('${TABLE_PATH}')"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT iceberg_drop_metadata_anchor_fail"
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --iceberg_delete_data_on_drop=1 --query "DROP TABLE ${DB}.t SYNC" 2>&1 | grep -o -m1 "FAULT_INJECTED"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT iceberg_drop_metadata_anchor_fail"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${DB}.t"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB}"
rm -rf "${TABLE_PATH}"

# Atomic table whose database name is reused by a Memory database while its drop is retried: still retried.
DB_ATOMIC="${DB}_atomic"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB_ATOMIC} ENGINE = Atomic"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_ATOMIC}.t (x Int32) ENGINE = IcebergLocal('${TABLE_PATH}')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${DB_ATOMIC}.t SETTINGS allow_insert_into_iceberg = 1 VALUES (1)"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT iceberg_drop_first_data_delete_fail"
${CLICKHOUSE_CLIENT} --iceberg_delete_data_on_drop=1 --database_atomic_wait_for_drop_and_detach_synchronously=0 --query "DROP DATABASE ${DB_ATOMIC}"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB_ATOMIC} ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT iceberg_drop_metadata_anchor_fail"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT iceberg_drop_first_data_delete_fail"
for _ in {1..120}; do
    [ -z "$(find "${TABLE_PATH}" -type f ! -path "${TABLE_PATH}metadata/*" 2>/dev/null)" ] && break
    sleep 0.5
done
find "${TABLE_PATH}" -type f ! -path "${TABLE_PATH}metadata/*" 2>/dev/null | wc -l
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT iceberg_drop_metadata_anchor_fail"
for _ in {1..120}; do [ -z "$(find "${TABLE_PATH}" -type f 2>/dev/null)" ] && break; sleep 0.5; done
find "${TABLE_PATH}" -type f 2>/dev/null | wc -l
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB_ATOMIC}"
rm -rf "${TABLE_PATH}"

# Atomic table whose drop is retried after its files were deleted, as by another replica, and another table was
# created at its path: the other table keeps its files.
DB_RETRY="${DB}_retry"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB_RETRY} ENGINE = Atomic"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_RETRY}.t (x Int32) ENGINE = IcebergLocal('${TABLE_PATH}')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${DB_RETRY}.t SETTINGS allow_insert_into_iceberg = 1 VALUES (1)"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT iceberg_drop_metadata_anchor_fail"
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --iceberg_delete_data_on_drop=1 --query "DROP TABLE ${DB_RETRY}.t SYNC" &
for _ in {1..120}; do
    [ -z "$(find "${TABLE_PATH}" -type f ! -path "${TABLE_PATH}metadata/*" 2>/dev/null)" ] && break
    sleep 0.5
done
[ -z "$(find "${TABLE_PATH}" -type f ! -path "${TABLE_PATH}metadata/*" 2>/dev/null)" ] \
    && [ -n "$(find "${TABLE_PATH}metadata" -name '*.metadata.json' 2>/dev/null)" ] && echo "retry: data deleted, metadata kept"
rm -rf "${TABLE_PATH}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_RETRY}.t2 (x Int32) ENGINE = IcebergLocal('${TABLE_PATH}')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${DB_RETRY}.t2 SETTINGS allow_insert_into_iceberg = 1 VALUES (3)"
FILES_BEFORE=$(cd "${TABLE_PATH}" && find . -type f | sort)
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT iceberg_drop_metadata_anchor_fail"
wait
[ "$(cd "${TABLE_PATH}" && find . -type f | sort)" = "${FILES_BEFORE}" ] && echo "retry: files kept"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM ${DB_RETRY}.t2"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB_RETRY}"
rm -rf "${TABLE_PATH}"
