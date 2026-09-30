#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_narrow/"
WIDE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_wide/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS t_signed;
        DROP TABLE IF EXISTS t_u64;
        DROP TABLE IF EXISTS t_nested_u64;
        DROP TABLE IF EXISTS t_wide"
    rm -rf "${TABLE_PATH}" "${WIDE_PATH}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    CREATE TABLE t_signed (a Int32, b Int64, c Array(Int32)) ENGINE = IcebergLocal('${TABLE_PATH}');
    INSERT INTO t_signed VALUES (1, 2, [3]);"

# The table already exists at the path, so these do not create a schema: they are ClickHouse tables
# with `UInt64` columns over the existing Iceberg fields, as older releases created them.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE IF NOT EXISTS t_u64 (a Int32, b UInt64, c Array(Int32)) ENGINE = IcebergLocal('${TABLE_PATH}');
    CREATE TABLE IF NOT EXISTS t_nested_u64 (a Int32, b Int64, c Array(UInt64)) ENGINE = IcebergLocal('${TABLE_PATH}');"

for table in t_u64 t_nested_u64
do
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${table} VALUES (1, 2, [3])" 2>&1 \
        | grep -o "Cannot write column [a-z]* of type [A-Za-z0-9()]*"
done

${CLICKHOUSE_CLIENT} --query "SELECT * FROM t_signed"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    CREATE TABLE t_wide (u UInt32) ENGINE = IcebergLocal('${WIDE_PATH}');
    INSERT INTO t_wide VALUES (4000000000);
    SELECT u FROM t_wide;
    SELECT u FROM icebergLocal('${WIDE_PATH}');"
