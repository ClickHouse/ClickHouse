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

rm -rf "${TABLE_PATH}"
