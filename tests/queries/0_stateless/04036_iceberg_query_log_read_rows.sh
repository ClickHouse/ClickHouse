#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for: system.query_log.read_rows = 0 for Iceberg reads
# IcebergIterator::next() was not calling the FileProgress callback and
# IcebergDataObjectInfo was not populating ObjectMetadata.size_bytes.
# This verifies that read_rows is correctly tracked when reading Iceberg tables.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ICEBERG_TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_04036"

# Cleanup from any previous run
rm -rf "${ICEBERG_TABLE_PATH}"

# Create a local Iceberg table and insert a known number of rows
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    CREATE TABLE t_04036 (c0 Int32) ENGINE = IcebergLocal('${ICEBERG_TABLE_PATH}');
    INSERT INTO t_04036 SELECT number FROM numbers(100);
"

# Run a uniquely-commented SELECT so we can identify it in system.query_log.
# optimize_count_from_files = 0 forces actual Parquet file reads instead of
# using the manifest record_count to answer the query without reading data.
${CLICKHOUSE_CLIENT} --query "
    SELECT /* 04036_iceberg_read_rows_test */ sum(c0)
    FROM icebergLocal('${ICEBERG_TABLE_PATH}')
    SETTINGS optimize_count_from_files = 0
" > /dev/null

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

# Verify read_rows > 0: the regression caused this to be 0 even with all
# pruning disabled, because the FileProgress callback was never invoked and
# ObjectMetadata was not populated from the Iceberg manifest.
${CLICKHOUSE_CLIENT} --query "
    SELECT read_rows > 0
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND query LIKE '%04036_iceberg_read_rows_test%'
      AND type = 'QueryFinish'
      AND event_date >= yesterday()
    ORDER BY event_time DESC
    LIMIT 1
"

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04036"
rm -rf "${ICEBERG_TABLE_PATH}"
