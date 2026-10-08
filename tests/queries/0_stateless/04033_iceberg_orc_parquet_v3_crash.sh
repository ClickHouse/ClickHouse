#!/usr/bin/env bash
# Tags: no-fasttest
# Reproduces https://github.com/ClickHouse/ClickHouse/issues/88126
# Parquet V3 reader crashes (SEGV) when reading an Iceberg table that contains
# a Parquet data file, while the table is configured with format = 'ORC'.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ICEBERG_TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_orc_v3"

# Cleanup
rm -rf "${ICEBERG_TABLE_PATH}"

# First insert creates a Parquet data file (default format).
# Second insert creates an ORC data file (table format).
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    CREATE TABLE t_orc_v3 (c0 String) ENGINE = IcebergLocal('${ICEBERG_TABLE_PATH}', 'ORC');
    INSERT INTO TABLE FUNCTION icebergLocal('${ICEBERG_TABLE_PATH}', 'Parquet', 'c0 String') (c0) SELECT 'a';
    INSERT INTO t_orc_v3 (c0) SELECT 'b';
"

# This should not crash (the native Parquet reader is always used).
${CLICKHOUSE_CLIENT} --query "
    SELECT c0 FROM icebergLocal('${ICEBERG_TABLE_PATH}', 'ORC', 'c0 String') ORDER BY c0;
"

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_orc_v3"
rm -rf "${ICEBERG_TABLE_PATH}"
