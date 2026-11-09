#!/usr/bin/env bash
# Tags: no-fasttest
# Requires `IcebergLocal` (USE_AVRO).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_05396"
MEMORY_DATABASE="${CLICKHOUSE_DATABASE}_05396"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${MEMORY_DATABASE}"
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

# In a `Memory` database the inner table of a materialized view is dropped together with the view.
${CLICKHOUSE_CLIENT} --query "
    CREATE DATABASE ${MEMORY_DATABASE} ENGINE = Memory;
    CREATE TABLE ${MEMORY_DATABASE}.src (x Int64) ENGINE = Memory;
    CREATE MATERIALIZED VIEW ${MEMORY_DATABASE}.mv ENGINE = IcebergLocal('${TABLE_PATH}/') AS SELECT x FROM ${MEMORY_DATABASE}.src;
    INSERT INTO ${MEMORY_DATABASE}.src SETTINGS allow_insert_into_iceberg = 1 VALUES (1);
"
echo "before drop: $(find "${TABLE_PATH}" -type f 2>/dev/null | wc -l | awk '{print ($1 > 0) ? "files present" : "no files"}')"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${MEMORY_DATABASE}.mv SYNC SETTINGS data_lake_delete_data_on_drop = 1"
echo "after drop: $(find "${TABLE_PATH}" -type f 2>/dev/null | wc -l | awk '{print ($1 > 0) ? "files kept" : "files deleted"}')"
