#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS test_01150"
$CLICKHOUSE_CLIENT --query "CREATE DATABASE test_01150"

$CLICKHOUSE_CLIENT --query "CREATE TABLE test_01150.t1 (x UInt64, s Array(Nullable(String))) ENGINE = Memory"
$CLICKHOUSE_CLIENT --query "CREATE TABLE test_01150.t2 (x UInt64, s Array(Nullable(String))) ENGINE = Memory"

function thread_detach_attach {
    local TIMELIMIT=$((SECONDS+20))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query "DETACH DATABASE test_01150" 2>&1 | grep -v -F -e 'Received exception from server' -e 'Code: 219' -e 'Code: 741' -e '(query: '
        sleep 0.0$RANDOM
        $CLICKHOUSE_CLIENT --query "ATTACH DATABASE test_01150" 2>&1 | grep -v -F -e 'Received exception from server' -e 'Code: 82' -e 'Code: 741' -e '(query: '
        sleep 0.0$RANDOM
    done
}

function thread_rename {
    local TIMELIMIT=$((SECONDS+20))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query "RENAME TABLE test_01150.t1 TO test_01150.t2_tmp, test_01150.t2 TO test_01150.t1, test_01150.t2_tmp TO test_01150.t2" 2>&1 | grep -v -F -e 'Received exception from server' -e '(query: ' | grep -v -P 'Code: (81|60|57|521|741)'
        sleep 0.0$RANDOM
        $CLICKHOUSE_CLIENT --query "RENAME TABLE test_01150.t2 TO test_01150.t1, test_01150.t2_tmp TO test_01150.t2" 2>&1 | grep -v -F -e 'Received exception from server' -e '(query: ' | grep -v -P 'Code: (81|60|57|521|741)'
        sleep 0.0$RANDOM
        $CLICKHOUSE_CLIENT --query "RENAME TABLE test_01150.t2_tmp TO test_01150.t2" 2>&1 | grep -v -F -e 'Received exception from server' -e '(query: ' | grep -v -P 'Code: (81|60|57|521|741)'
        sleep 0.0$RANDOM
    done
}

thread_detach_attach &
thread_rename &
wait
sleep 1

$CLICKHOUSE_CLIENT --query "DETACH DATABASE IF EXISTS test_01150"
$CLICKHOUSE_CLIENT --query "ATTACH DATABASE IF NOT EXISTS test_01150"
$CLICKHOUSE_CLIENT --query "DROP DATABASE test_01150"
