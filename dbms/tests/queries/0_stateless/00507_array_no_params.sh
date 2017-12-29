#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test.foo;"
# Missing arguments for array, table not created
$CLICKHOUSE_CLIENT -q "CREATE TABLE test.foo (a Array) Engine=Memory;" 2&>/dev/null
$CLICKHOUSE_CLIENT -q "SELECT 'Still alive';"
