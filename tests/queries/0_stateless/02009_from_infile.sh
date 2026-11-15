#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

[ -e "${CLICKHOUSE_TMP}"/test_infile.gz ] && rm "${CLICKHOUSE_TMP}"/test_infile.gz
[ -e "${CLICKHOUSE_TMP}"/test_infile ] && rm "${CLICKHOUSE_TMP}"/test_infile

echo "Hello" > "${CLICKHOUSE_TMP}"/test_infile

gzip "${CLICKHOUSE_TMP}"/test_infile

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_infile;"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_infile (word String) ENGINE=Memory();"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_infile FROM INFILE '${CLICKHOUSE_TMP}/test_infile.gz' FORMAT CSV;"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_infile;"

# if it not fails, select will print information
${CLICKHOUSE_LOCAL} --query "CREATE TABLE test_infile (word String) ENGINE=Memory(); INSERT INTO test_infile FROM INFILE '${CLICKHOUSE_TMP}/test_infile.gz' FORMAT CSV; SELECT * from test_infile;"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=DROP+TABLE" -d 'IF EXISTS test_infile_url'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=CREATE" -d 'TABLE test_infile_url (x String) ENGINE = Memory'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "INSERT INTO test_infile_url FROM INFILE '${CLICKHOUSE_TMP}/test_infile.gz' FORMAT CSV" 2>&1 | grep -q "UNKNOWN_TYPE_OF_QUERY" && echo "Correct URL" || echo 'Fail'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'SELECT x FROM test_infile_url'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=DROP+TABLE" -d 'test_infile_url'
