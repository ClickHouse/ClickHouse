#!/usr/bin/env bash
# Tags: no-unbundled, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_nullable_arrays"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_nullable_arrays (arr1 Array(Nullable(UInt32)), arr2 Array(Nullable(String)), arr3 Array(Nullable(Float32))) ENGINE=Memory()"
${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_nullable_arrays VALUES ([1,NULL,2],[NULL,'Some string',NULL],[0.00,NULL,42.42]), ([NULL],[NULL],[NULL]), ([],[],[])"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_nullable_arrays FORMAT Arrow" > "${CLICKHOUSE_TMP}"/nullable_arrays.arrow

cat "${CLICKHOUSE_TMP}"/nullable_arrays.arrow | ${CLICKHOUSE_CLIENT} -q "INSERT INTO arrow_nullable_arrays FORMAT Arrow"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_nullable_arrays"
${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_nullable_arrays"
