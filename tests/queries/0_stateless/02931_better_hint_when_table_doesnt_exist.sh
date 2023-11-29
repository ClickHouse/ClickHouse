#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS test_02931;"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE test_02931;"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02931.query_log;" 2>&1 | grep -q "Table test_02931.query_log does not exist" && echo 'OK' || echo 'FAIL'
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02931.query_log;" 2>&1 | grep -q " Maybe you meant system.query_log?" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02931.foo_bar;" 2>&1 | grep -q "Table test_02931.foo_bar does not exist" && echo 'OK' || echo 'FAIL'
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02931.foo_bar;" 2>&1 | grep " Maybe you meant system.foo_bar?" && echo 'FAIL' || echo 'OK'

$CLICKHOUSE_CLIENT -q "SELECT * FROM system.querylog;" 2>&1 | grep -q " Maybe you meant system.query_log?" && echo 'OK' || echo 'FAIL'
$CLICKHOUSE_CLIENT -q "SELECT * FROM querylog;" 2>&1 | grep -q " Maybe you meant system.query_log?" && echo 'OK' || echo 'FAIL'


$CLICKHOUSE_CLIENT -q "DROP DATABASE test_02931;"
