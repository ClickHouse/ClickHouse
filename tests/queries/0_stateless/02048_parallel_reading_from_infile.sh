#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

[ -e "${CLICKHOUSE_TMP}"/test_infile_parallel.gz ] && rm "${CLICKHOUSE_TMP}"/test_infile_parallel.gz
[ -e "${CLICKHOUSE_TMP}"/test_infile_parallel ] && rm "${CLICKHOUSE_TMP}"/test_infile_parallel
[ -e "${CLICKHOUSE_TMP}"/test_infile_parallel ] && rm "${CLICKHOUSE_TMP}"/test_infile_parallel_1
[ -e "${CLICKHOUSE_TMP}"/test_infile_parallel ] && rm "${CLICKHOUSE_TMP}"/test_infile_parallel_2

echo "Hello" > "${CLICKHOUSE_TMP}"/test_infile_parallel
echo "Hello" > "${CLICKHOUSE_TMP}"/test_infile_parallel_1
echo "Hello" > "${CLICKHOUSE_TMP}"/test_infile_parallel_2

gzip "${CLICKHOUSE_TMP}"/test_infile_parallel

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_infile_parallel;"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_infile_parallel (word String) ENGINE=Memory();"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_infile_parallel FROM INFILE '${CLICKHOUSE_TMP}/test_infile_parallel*' FORMAT CSV;"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test_infile_parallel;"

${CLICKHOUSE_LOCAL} --multiquery <<EOF
DROP TABLE IF EXISTS test_infile_parallel; 
CREATE TABLE test_infile_parallel (word String) ENGINE=Memory(); 
INSERT INTO test_infile_parallel FROM INFILE '${CLICKHOUSE_TMP}/test_infile_parallel*' FORMAT CSV;
SELECT count() FROM test_infile_parallel;
EOF
