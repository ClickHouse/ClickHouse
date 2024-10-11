#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "CREATE TABLE t_async_insert_deadlock (a UInt64) ENGINE = Log"

echo '{"a": 1}' | $CLICKHOUSE_CLIENT --async_insert 1 --wait_for_async_insert 1 --query "INSERT INTO t_async_insert_deadlock FORMAT JSONEachRow"

$CLICKHOUSE_CLIENT --query "SELECT * FROM t_async_insert_deadlock ORDER BY a"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_async_insert_deadlock"
