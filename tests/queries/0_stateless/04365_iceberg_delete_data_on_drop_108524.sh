#!/usr/bin/env bash
# Tags: no-fasttest

# DROP TABLE with iceberg_delete_data_on_drop = 1 deletes the table files: https://github.com/ClickHouse/ClickHouse/issues/108524

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
rm -rf "${TABLE_PATH}"

function create_table()
{
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (x Int32) ENGINE = IcebergLocal('${TABLE_PATH}') $1"
}

# Setting on: the files are deleted, so the table can be created again at the same path.
create_table
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} SETTINGS allow_insert_into_iceberg = 1 VALUES (1)"
${CLICKHOUSE_CLIENT} --iceberg_delete_data_on_drop=1 --query "DROP TABLE ${TABLE} SYNC"
echo "on: $(ls -A "${TABLE_PATH}" 2>/dev/null | wc -l) files left"
create_table && echo "on: re-created"

# Setting off (default): the files are kept.
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} SETTINGS allow_insert_into_iceberg = 1 VALUES (1)"
FILES_BEFORE=$(cd "${TABLE_PATH}" && find . -type f | sort)
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE} SYNC"
[ "$(cd "${TABLE_PATH}" && find . -type f | sort)" = "${FILES_BEFORE}" ] && echo "off: files kept"

# A leftover version-hint.text without any metadata file still occupies the path.
rm -rf "${TABLE_PATH}"
mkdir -p "${TABLE_PATH}metadata"
echo 1 > "${TABLE_PATH}metadata/version-hint.text"
create_table "SETTINGS iceberg_use_version_hint = 1" 2>&1 | grep -o -m1 "TABLE_ALREADY_EXISTS"
[ -f "${TABLE_PATH}metadata/v1.metadata.json" ] || echo "hint: no metadata written"

# Setting on: a table stored inside the dropped table's directory keeps its files.
rm -rf "${TABLE_PATH}"
create_table
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} SETTINGS allow_insert_into_iceberg = 1 VALUES (1)"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE}_inner (x Int32) ENGINE = IcebergLocal('${TABLE_PATH}inner/')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE}_inner SETTINGS allow_insert_into_iceberg = 1 VALUES (2)"
${CLICKHOUSE_CLIENT} --iceberg_delete_data_on_drop=1 --query "DROP TABLE ${TABLE} SYNC"
echo "inner: $(find "${TABLE_PATH}" -type f ! -path "${TABLE_PATH}inner/*" | wc -l) outer files left"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM ${TABLE}_inner"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}_inner SYNC"

# Setting on: the files are already deleted, as by another replica, and a table created at the path since keeps its files.
rm -rf "${TABLE_PATH}"
create_table
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} SETTINGS allow_insert_into_iceberg = 1 VALUES (1)"
rm -rf "${TABLE_PATH}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE}_new (x Int32) ENGINE = IcebergLocal('${TABLE_PATH}')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE}_new SETTINGS allow_insert_into_iceberg = 1 VALUES (3)"
${CLICKHOUSE_CLIENT} --iceberg_delete_data_on_drop=1 --query "DROP TABLE ${TABLE} SYNC"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM ${TABLE}_new"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}_new SYNC"

# Setting on: the latest metadata file at the path is another table's, written by another writer under a name this
# table used before, and that table keeps its files. Also when this table is a format version 1 one without `table-uuid`.
for format_version in 2 1; do
    rm -rf "${TABLE_PATH}"
    create_table "SETTINGS iceberg_format_version = ${format_version}"
    if [ "${format_version}" = 1 ]; then
        # The format version 1 metadata written by ClickHouse has no `schema`, which is logged as a warning.
        ${CLICKHOUSE_CLIENT} --send_logs_level=error --query "INSERT INTO ${TABLE} SETTINGS allow_insert_into_iceberg = 1 VALUES (1)"
        sed -i -z 's/,\n *"table-uuid" : "[^"]*"//' "${TABLE_PATH}metadata/"*.metadata.json
        ${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${TABLE} PERMANENTLY"
        ${CLICKHOUSE_CLIENT} --query "ATTACH TABLE ${TABLE}"
        ${CLICKHOUSE_CLIENT} --send_logs_level=error --use_iceberg_metadata_files_cache=0 --query "SELECT x FROM ${TABLE}"
    else
        ${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} SETTINGS allow_insert_into_iceberg = 1 VALUES (1)"
        ${CLICKHOUSE_CLIENT} --query "SELECT x FROM ${TABLE}"
    fi
    OWN_METADATA="${TABLE_PATH%/}_own/"
    mkdir -p "${OWN_METADATA}" && mv "${TABLE_PATH}metadata/"*.metadata.json "${OWN_METADATA}"
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE}_other (x Int32) ENGINE = IcebergLocal('${TABLE_PATH}') SETTINGS iceberg_use_version_hint = 1"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE}_other SETTINGS allow_insert_into_iceberg = 1 VALUES (2)"
    cp "${OWN_METADATA}v1.metadata.json" "${TABLE_PATH}metadata/"
    ${CLICKHOUSE_CLIENT} --send_logs_level=fatal --iceberg_delete_data_on_drop=1 --query "DROP TABLE ${TABLE} SYNC"
    if [ "${format_version}" = 2 ]; then
        # Selected by its `table-uuid`, this table deletes its own metadata, and the other table's version hint is kept.
        OWN_UUID=$(grep -o -m1 '"table-uuid" : "[^"]*"' "${OWN_METADATA}v1.metadata.json" | cut -d'"' -f4)
        ${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "CREATE TABLE ${TABLE}_pinned ENGINE = IcebergLocal('${TABLE_PATH}') SETTINGS iceberg_metadata_table_uuid = '${OWN_UUID}'"
        ${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --iceberg_delete_data_on_drop=1 --query "DROP TABLE ${TABLE}_pinned SYNC"
        [ -f "${TABLE_PATH}metadata/v1.metadata.json" ] || echo "pinned: own metadata deleted"
    fi
    ${CLICKHOUSE_CLIENT} --query "SELECT x FROM ${TABLE}_other"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}_other SYNC"
    rm -rf "${OWN_METADATA}"
done

# Setting on: a table not read since it was attached is dropped after its files are deleted, as by another replica.
rm -rf "${TABLE_PATH}"
create_table
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} SETTINGS allow_insert_into_iceberg = 1 VALUES (1)"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${TABLE} PERMANENTLY"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE ${TABLE}"
rm -rf "${TABLE_PATH}"
${CLICKHOUSE_CLIENT} --iceberg_delete_data_on_drop=1 --query "DROP TABLE ${TABLE} SYNC" && echo "attached: dropped"

rm -rf "${TABLE_PATH}"
