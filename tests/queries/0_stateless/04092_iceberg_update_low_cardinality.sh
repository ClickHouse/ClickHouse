#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ICEBERG_TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_t0"

# Cleanup
rm -rf "${ICEBERG_TABLE_PATH}"

# Test 1: ALTER UPDATE on IcebergLocal with default (Parquet) format.
# The position delete file writer used to crash because `_iceberg_metadata_file_path`
# arrives as LowCardinality(String) from the pipeline, but the Avro/Parquet serializer
# expected plain String.
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    CREATE TABLE t0 (c0 Int) ENGINE = IcebergLocal('${ICEBERG_TABLE_PATH}');
    INSERT INTO t0 VALUES (1), (2), (3);
    ALTER TABLE t0 UPDATE c0 = 10 WHERE c0 = 1;
    SELECT c0 FROM t0 ORDER BY c0;
"

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t0"
rm -rf "${ICEBERG_TABLE_PATH}"

# Test 2: ALTER UPDATE on IcebergLocal with Avro format.
# This was the original reproduction case from issue #101916 (server crash). With
# the followup fix for issue #102508, non-Parquet mutations are rejected outright
# (the previous "no crash" outcome silently corrupted the table). The test now
# verifies that the rejection happens cleanly without crashing.
${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg=1 --query "
    CREATE TABLE t0 (c0 Int) ENGINE = IcebergLocal('${ICEBERG_TABLE_PATH}', 'Avro');
    INSERT INTO t0 VALUES (1);
"
${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg=1 --query "ALTER TABLE t0 UPDATE c0 = 1 WHERE TRUE" 2>&1 | grep -oE 'Code: [0-9]+' | head -1

echo "OK"

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t0"
rm -rf "${ICEBERG_TABLE_PATH}"
