#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (c0 Int32) ENGINE = IcebergLocal('${TABLE_PATH}')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (2)"

echo -n 2 > "${TABLE_PATH}metadata/version-hint.text"

echo "without version hint:"
${CLICKHOUSE_CLIENT} --query "SELECT c0 FROM ${TABLE} ORDER BY c0"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} MODIFY SETTING iceberg_use_version_hint = 1"
echo "with version hint:"
${CLICKHOUSE_CLIENT} --query "SELECT c0 FROM ${TABLE} ORDER BY c0"
${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE ${TABLE}" | grep -o "SETTINGS iceberg_use_version_hint = 1"

${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE ${TABLE}"
echo "with version hint after ATTACH:"
${CLICKHOUSE_CLIENT} --query "SELECT c0 FROM ${TABLE} ORDER BY c0"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} RESET SETTING iceberg_use_version_hint"
echo "after RESET SETTING:"
${CLICKHOUSE_CLIENT} --query "SELECT c0 FROM ${TABLE} ORDER BY c0"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} MODIFY SETTING iceberg_format_version = 1" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} ADD COLUMN c1 Int32, MODIFY SETTING iceberg_use_version_hint = 1" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH}"
