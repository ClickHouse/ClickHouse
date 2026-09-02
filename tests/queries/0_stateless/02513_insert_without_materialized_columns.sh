#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


FILE_NAME="${CLICKHOUSE_DATABASE}_test.native.zstd"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test (a Int64, b Int64 MATERIALIZED a) ENGINE = MergeTree() PRIMARY KEY tuple()"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test VALUES (1)"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM test"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM test INTO OUTFILE '${CLICKHOUSE_TMP}/${FILE_NAME}' FORMAT Native"

${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE test"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test FROM INFILE '${CLICKHOUSE_TMP}/${FILE_NAME}'"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM test"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test"

rm -f "${CLICKHOUSE_TMP}/${FILE_NAME}"
