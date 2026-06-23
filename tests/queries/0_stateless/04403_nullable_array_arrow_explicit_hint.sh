#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_TEST_UNIQUE_NAME}_nullable_array_arrow"
DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.arrow"

${CLICKHOUSE_CLIENT} --allow_experimental_nullable_array_type=1 --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --allow_experimental_nullable_array_type=1 --query "CREATE TABLE ${TABLE} (a Nullable(Array(UInt8))) ENGINE = Memory"

${CLICKHOUSE_CLIENT} --allow_experimental_nullable_array_type=1 --query "
    SELECT CAST(NULL AS Nullable(Array(UInt8))) AS a
    UNION ALL
    SELECT CAST([1, 2] AS Nullable(Array(UInt8))) AS a
    FORMAT Arrow" > "${DATA_FILE}"

${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} FROM INFILE '${DATA_FILE}' FORMAT Arrow"

${CLICKHOUSE_CLIENT} --allow_experimental_nullable_array_type=1 --query "
    SELECT throwIf(count() != 2) FROM ${TABLE} FORMAT Null;
    SELECT throwIf(countIf(isNull(a)) != 1) FROM ${TABLE} FORMAT Null;
    SELECT throwIf(countIf(NOT isNull(a) AND a = [1, 2]) != 1) FROM ${TABLE}
    FORMAT Null"

${CLICKHOUSE_CLIENT} --allow_experimental_nullable_array_type=1 --query "DROP TABLE ${TABLE}"
