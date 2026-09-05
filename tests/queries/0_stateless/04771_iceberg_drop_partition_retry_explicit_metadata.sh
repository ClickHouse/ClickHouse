#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_drop_partition_explicit_metadata"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}" >/dev/null 2>&1
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (partition_key Int64)
    ENGINE = IcebergLocal('${TABLE_PATH}')
    PARTITION BY partition_key
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (2)"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE}
    ENGINE = IcebergLocal('${TABLE_PATH}')
    SETTINGS iceberg_metadata_file_path = 'metadata/v2.metadata.json'
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE}
    ENGINE = IcebergLocal('${TABLE_PATH}')
"
${CLICKHOUSE_CLIENT} --query "SELECT partition_key FROM ${TABLE} ORDER BY partition_key"
