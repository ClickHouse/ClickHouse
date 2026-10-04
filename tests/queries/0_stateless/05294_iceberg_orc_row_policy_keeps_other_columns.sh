#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# - no-fasttest: uses Iceberg
# - no-parallel-replicas: see the comment in `04071_iceberg_orc_prewhere_crash.sh`

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An Iceberg table with format `Parquet` and ORC data files: the source applies the row policy
# after the ORC reader, because ORC does not support PREWHERE. The columns that the row policy
# does not use must stay in the block, in any order of the selected columns.

ICEBERG_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_05294"
rm -rf "${ICEBERG_PATH}"

${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    SET input_format_parquet_use_native_reader_v3 = 1;
    CREATE TABLE t (k UInt64, a UInt64, s String) ENGINE = IcebergLocal('${ICEBERG_PATH}', 'Parquet');
    INSERT INTO TABLE FUNCTION icebergLocal('${ICEBERG_PATH}', 'ORC', 'k UInt64, a UInt64, s String')
        SELECT number, number % 3, toString(number * 10) FROM numbers(10);
"

function check()
{
    ${CLICKHOUSE_CLIENT} --query "
        SET input_format_parquet_use_native_reader_v3 = 1;
        SELECT k FROM t ORDER BY k;
        SELECT s, k FROM t ORDER BY k;
        SELECT k, s FROM t ORDER BY k;
        SELECT s FROM t ORDER BY s;
        SELECT count() FROM t;
    "
}

echo "-- USING a"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY policy_05294 ON t USING a TO ALL"
check

echo "-- USING a > 0"
${CLICKHOUSE_CLIENT} --query "ALTER ROW POLICY policy_05294 ON t USING a > 0"
check

${CLICKHOUSE_CLIENT} --query "
    DROP ROW POLICY policy_05294 ON t;
    DROP TABLE t;
"
rm -rf "${ICEBERG_PATH}"
