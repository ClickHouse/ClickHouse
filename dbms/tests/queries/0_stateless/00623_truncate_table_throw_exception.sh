#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS test_truncate;"

${CLICKHOUSE_CLIENT} --query "CREATE DATABASE test_truncate;"

${CLICKHOUSE_CLIENT} --query "SELECT '========Before Truncate========';"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_truncate.test_view_depend (s String) ENGINE = Log;"
${CLICKHOUSE_CLIENT} --query "CREATE VIEW test_truncate.test_view AS SELECT * FROM test_truncate.test_view_depend;"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test_truncate.test_view_depend VALUES('test_string');"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_truncate.test_view;"

${CLICKHOUSE_CLIENT} --query "SELECT '========Execute Truncate========';"
echo `${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE test_truncate.test_view;" --server_logs_file=/dev/null 2>&1 | grep -c "Code: 48.*Truncate is not supported by storage View"`

${CLICKHOUSE_CLIENT} --query "SELECT '========After Truncate========';"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_truncate.test_view;"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS test_truncate;"