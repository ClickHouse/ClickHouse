#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS orc_nullable_arrays"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE orc_nullable_arrays (arr1 Array(Nullable(UInt32)), arr2 Array(Nullable(String)), arr3 Array(Nullable(Decimal(4, 2)))) ENGINE=Memory()"
${CLICKHOUSE_CLIENT} --query="INSERT INTO orc_nullable_arrays VALUES ([1,NULL,2],[NULL,'Some string',NULL],[0.00,NULL,42.42]), ([NULL],[NULL],[NULL]), ([],[],[])"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM orc_nullable_arrays FORMAT ORC" > "${CLICKHOUSE_TMP}"/nullable_arrays.orc

cat "${CLICKHOUSE_TMP}"/nullable_arrays.orc | ${CLICKHOUSE_CLIENT} -q "INSERT INTO orc_nullable_arrays FORMAT ORC"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM orc_nullable_arrays"
${CLICKHOUSE_CLIENT} --query="DROP TABLE orc_nullable_arrays"
