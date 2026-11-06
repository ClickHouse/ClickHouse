#!/usr/bin/env bash
# Tags: no-fasttest
# Requires `IcebergLocal` (USE_AVRO).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_05395"

cleanup()
{
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE src (x Int64) ENGINE = IcebergLocal('${TABLE_PATH}/');
    INSERT INTO src SETTINGS allow_insert_into_iceberg = 1 VALUES (1);
    DROP TABLE src SYNC SETTINGS data_lake_delete_data_on_drop = 0;
    CREATE TABLE t (x Int64) AS icebergLocal('${TABLE_PATH}/');
"
echo "before drop: $(find "${TABLE_PATH}" -type f 2>/dev/null | wc -l | awk '{print ($1 > 0) ? "files present" : "no files"}')"

# The table is dropped without being read, so the table function has not been resolved yet.
${CLICKHOUSE_CLIENT} --query "DROP TABLE t SYNC SETTINGS data_lake_delete_data_on_drop = 1"
echo "after drop: $(find "${TABLE_PATH}" -type f 2>/dev/null | wc -l | awk '{print ($1 > 0) ? "files kept" : "files deleted"}')"
