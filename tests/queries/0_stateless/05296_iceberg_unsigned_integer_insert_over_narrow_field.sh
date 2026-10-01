#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_narrow/"
WIDE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_wide/"
NEW_PATH="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_new/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS t_signed;
        DROP TABLE IF EXISTS t_u64;
        DROP TABLE IF EXISTS t_wide"
    rm -rf "${TABLE_PATH}" "${WIDE_PATH}" "${NEW_PATH}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    CREATE TABLE t_signed (a Int32, b Int64, c Array(Int32)) ENGINE = IcebergLocal('${TABLE_PATH}');
    INSERT INTO t_signed VALUES (1, 2, [3]);"

# `UInt64` cannot be stored in Iceberg, so a table with such a column cannot be created, whether the
# Iceberg table exists at the path already or not.
for path in "${TABLE_PATH}" "${NEW_PATH}"
do
    for columns in "a Int32, b UInt64, c Array(Int32)" "a Int32, b Int64, c Array(UInt64)"
    do
        ${CLICKHOUSE_CLIENT} --query "CREATE TABLE IF NOT EXISTS t_u64 (${columns}) ENGINE = IcebergLocal('${path}')" 2>&1 \
            | grep -o "Column [a-z]* of type [A-Za-z0-9()]* cannot be stored in Iceberg" | head -n1
    done
done
ls "${NEW_PATH}" 2>/dev/null

${CLICKHOUSE_CLIENT} --query "SELECT * FROM t_signed"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    CREATE TABLE t_wide (u UInt32) ENGINE = IcebergLocal('${WIDE_PATH}');
    INSERT INTO t_wide VALUES (4000000000);
    SELECT u FROM t_wide;
    SELECT u FROM icebergLocal('${WIDE_PATH}');"
