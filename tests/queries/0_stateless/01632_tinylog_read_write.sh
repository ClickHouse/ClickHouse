#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --multiquery --query "DROP TABLE IF EXISTS test; CREATE TABLE IF NOT EXISTS test (x UInt64, s Array(Nullable(String))) ENGINE = TinyLog;"

function thread_select()
{
    $CLICKHOUSE_CLIENT --query "SELECT * FROM test FORMAT Null"
    sleep 0.0$RANDOM
}

function thread_insert()
{
    $CLICKHOUSE_CLIENT --query "INSERT INTO test VALUES (1, ['Hello'])"
    sleep 0.0$RANDOM
}

export -f thread_select
export -f thread_insert

# Do randomized queries and expect nothing extraordinary happens.

clickhouse_client_loop_timeout 10 thread_select &
clickhouse_client_loop_timeout 10 thread_select &
clickhouse_client_loop_timeout 10 thread_select &
clickhouse_client_loop_timeout 10 thread_select &

clickhouse_client_loop_timeout 10 thread_insert &
clickhouse_client_loop_timeout 10 thread_insert &
clickhouse_client_loop_timeout 10 thread_insert &
clickhouse_client_loop_timeout 10 thread_insert &

wait
echo "Done"

$CLICKHOUSE_CLIENT --multiquery --query "DROP TABLE IF EXISTS test;"
