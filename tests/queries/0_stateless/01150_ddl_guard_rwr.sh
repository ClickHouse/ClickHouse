#!/usr/bin/env bash

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS test_01150"
$CLICKHOUSE_CLIENT --query "CREATE DATABASE test_01150"

$CLICKHOUSE_CLIENT --query "CREATE TABLE test_01150.t1 (x UInt64, s Array(Nullable(String))) ENGINE = Memory"
$CLICKHOUSE_CLIENT --query "CREATE TABLE test_01150.t2 (x UInt64, s Array(Nullable(String))) ENGINE = Memory"

function thread_detach_attach {
    while true; do
        $CLICKHOUSE_CLIENT --query "DETACH DATABASE test_01150" 2>&1 | grep -v -F 'Received exception from server' | grep -v -P 'Code: (219)'
        sleep 0.0$RANDOM
        $CLICKHOUSE_CLIENT --query "ATTACH DATABASE test_01150" 2>&1 | grep -v -F 'Received exception from server' | grep -v -P 'Code: (82)'
        sleep 0.0$RANDOM
    done
}

function thread_rename {
    while true; do
        $CLICKHOUSE_CLIENT --query "RENAME TABLE test_01150.t1 TO test_01150.t2_tmp, test_01150.t2 TO test_01150.t1, test_01150.t2_tmp TO test_01150.t2" 2>&1 | grep -v -F 'Received exception from server' | grep -v -P 'Code: (81|60|57|521)'
        sleep 0.0$RANDOM
        $CLICKHOUSE_CLIENT --query "RENAME TABLE test_01150.t2 TO test_01150.t1, test_01150.t2_tmp TO test_01150.t2" 2>&1 | grep -v -F 'Received exception from server' | grep -v -P 'Code: (81|60|57|521)'
        sleep 0.0$RANDOM
        $CLICKHOUSE_CLIENT --query "RENAME TABLE test_01150.t2_tmp TO test_01150.t2" 2>&1 | grep -v -F 'Received exception from server' | grep -v -P 'Code: (81|60|57|521)'
        sleep 0.0$RANDOM
    done
}

export -f thread_detach_attach
export -f thread_rename

timeout 20 bash -c "thread_detach_attach" &
timeout 20 bash -c 'thread_rename' &
wait
sleep 1

$CLICKHOUSE_CLIENT --query "DETACH DATABASE IF EXISTS test_01150"
$CLICKHOUSE_CLIENT --query "ATTACH DATABASE IF NOT EXISTS test_01150"
$CLICKHOUSE_CLIENT --query "DROP DATABASE test_01150";
