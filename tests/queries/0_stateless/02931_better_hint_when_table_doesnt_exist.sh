#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS test_02931;"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE test_02931;"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_table_02931;"

$CLICKHOUSE_CLIENT -q "SELECT 1 FROM system.numbers LIMIT 1;" 2>&1

$CLICKHOUSE_CLIENT -q "SELECT 1 FROM test_02931.numbers LIMIT 1;" 2>&1 | grep -q -e "Table test_02931.numbers does not exist" -e "Maybe you meant system.numbers?" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02931.foo_bar;" 2>&1 | grep -q -e "Table test_02931.foo_bar does not exist" -v -e "Maybe you meant system.foo_bar?" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT -q "SELECT 1 FROM system.numbers2 LIMIT 1;" 2>&1 | grep -q -e "Maybe you meant system.numbers?" -e "Maybe you meant system.numbers?" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT -n --query="CREATE TABLE test_table_02931 (n Int) Engine=MergeTree() ORDER BY n;"
$CLICKHOUSE_CLIENT -n --query="INSERT INTO TABLE test_table_02931 VALUES(1);"
$CLICKHOUSE_CLIENT -n --query="SELECT * FROM test_table_02931 LIMIT 1;"
$CLICKHOUSE_CLIENT -n --query="SELECT * FROM test_02931.test_table_02931 LIMIT 1;" 2>&1 | grep -q -e "Table test_02931.test_table_02931 does not exist" -e "Maybe you meant default.test_table_02931?" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT -q "DROP TABLE test_table_02931;"
$CLICKHOUSE_CLIENT -q "DROP DATABASE test_02931;"
