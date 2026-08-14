#!/usr/bin/env bash
# Tags: no-fasttest

# Iceberg v3 writers must not add new position-delete files. ClickHouse mutations
# still write parquet position deletes, so DELETE/UPDATE on a format-version-3
# table (even without existing deletion vectors) must fail closed before any
# object writes.

# Force quieter logs: CI often pre-sets CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=warning,
# and ${VAR:-error} would keep that. IcebergMetadata logs a Warning when reading
# CH-written v3 metadata (v1 `schema` missing → v2 `schemas` fallback).
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=error

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (c0 Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    SETTINGS iceberg_format_version = 3
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1), (2), (3)"

# Fail closed: no position-delete files may be written for v3.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query \
    "ALTER TABLE ${TABLE} DELETE WHERE c0 = 1" 2>&1 | grep -o 'SUPPORT_IS_DISABLED' | head -n1
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query \
    "ALTER TABLE ${TABLE} UPDATE c0 = 10 WHERE c0 = 2" 2>&1 | grep -o 'SUPPORT_IS_DISABLED' | head -n1

# Rows must be unchanged (rejection happened before object writes).
${CLICKHOUSE_CLIENT} --query "SELECT count(), groupArray(c0) FROM (SELECT c0 FROM ${TABLE} ORDER BY c0)"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
