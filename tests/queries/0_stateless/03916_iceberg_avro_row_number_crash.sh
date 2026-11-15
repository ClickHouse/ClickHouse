#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ICEBERG_TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_t0"

# Cleanup
rm -rf "${ICEBERG_TABLE_PATH}"

# Create Iceberg table with Avro format and insert data
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    CREATE TABLE t0 (c0 Int) ENGINE = IcebergLocal('${ICEBERG_TABLE_PATH}', 'Avro');
    INSERT INTO t0 VALUES (1), (2), (3);
"

# This should not crash - selecting _row_number virtual column from IcebergLocal with Avro data
${CLICKHOUSE_CLIENT} --query "
    SELECT _row_number FROM icebergLocal('${ICEBERG_TABLE_PATH}') ORDER BY _row_number SETTINGS optimize_count_from_files = 0;
"

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t0"
rm -rf "${ICEBERG_TABLE_PATH}"
