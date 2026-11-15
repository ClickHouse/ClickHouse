#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_explicit_metadata"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (p Int64) ENGINE = IcebergLocal('${TABLE_PATH}') PARTITION BY p"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} SETTINGS allow_insert_into_iceberg = 1 VALUES (1)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} SETTINGS allow_insert_into_iceberg = 1 VALUES (2)"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"

# Check both historical and current metadata paths.
for VERSION in 2 3; do
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} ENGINE = IcebergLocal('${TABLE_PATH}') SETTINGS iceberg_metadata_file_path = 'metadata/v${VERSION}.metadata.json'"
    if ERROR=$(${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} DROP PARTITION 1 SETTINGS allow_insert_into_iceberg = 1" 2>&1); then
        echo 'Unexpected successful DROP PARTITION with explicit metadata'
        exit 1
    fi
    echo "${ERROR}" | grep -o 'BAD_ARGUMENTS' | head -1
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} ENGINE = IcebergLocal('${TABLE_PATH}')"
    ${CLICKHOUSE_CLIENT} --query "SELECT p FROM ${TABLE} ORDER BY p"
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.iceberg_history WHERE database = currentDatabase() AND table = '${TABLE}'"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
done

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} ENGINE = IcebergLocal('${TABLE_PATH}')"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} DROP PARTITION 1 SETTINGS allow_insert_into_iceberg = 1"
${CLICKHOUSE_CLIENT} --query "SELECT p FROM ${TABLE} ORDER BY p"
