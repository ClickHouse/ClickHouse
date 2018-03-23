#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

exception_pattern="Code: 48.*Method write is not supported by storage View"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.test;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.test_view;"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.test (s String) ENGINE = Log;"
${CLICKHOUSE_CLIENT} --query "CREATE VIEW test.test_view AS SELECT * FROM test.test;"

echo `${CLICKHOUSE_CLIENT} --query "INSERT INTO test.test_view VALUES('test_string');" 2>&1 | grep -c "$exception_pattern"`
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.test VALUES('test_string');"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test.test;"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.test;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.test_view;"
