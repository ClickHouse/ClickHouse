#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

[ -e "${CLICKHOUSE_TMP}"/test_infile.gz ] && rm "${CLICKHOUSE_TMP}"/test_infile.gz
[ -e "${CLICKHOUSE_TMP}"/test_infile ] && rm "${CLICKHOUSE_TMP}"/test_infile

echo "('Hello')" > "${CLICKHOUSE_TMP}"/test_infile

gzip "${CLICKHOUSE_TMP}"/test_infile

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_infile;"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_infile (word String) ENGINE=Memory();"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_infile FROM INFILE '${CLICKHOUSE_TMP}/test_infile.gz';"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_infile;"
