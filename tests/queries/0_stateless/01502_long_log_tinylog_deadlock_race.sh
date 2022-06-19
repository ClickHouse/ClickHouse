#!/usr/bin/env bash
# Tags: deadlock

set -e

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


function thread_create {
    while true; do
        $CLICKHOUSE_CLIENT --query "CREATE TABLE IF NOT EXISTS $1 (x UInt64, s Array(Nullable(String))) ENGINE = $2" 2>&1 | grep -v -F 'Received exception from server' | grep -v -P 'Code: (60|57)'
        sleep 0.0$RANDOM
    done
}

function thread_drop {
    while true; do
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $1" 2>&1 | grep -v -e 'Received exception from server' -e '^(query: ' | grep -v -P 'Code: (60|57)'
        sleep 0.0$RANDOM
    done
}

function thread_rename {
    while true; do
        $CLICKHOUSE_CLIENT --query "RENAME TABLE $1 TO $2" 2>&1 | grep -v -e 'Received exception from server' -e '^(query: ' | grep -v -P 'Code: (60|57)'
        sleep 0.0$RANDOM
    done
}

function thread_select {
    while true; do
        $CLICKHOUSE_CLIENT --query "SELECT * FROM $1 FORMAT Null" 2>&1 | grep -v -e 'Received exception from server' -e '^(query: ' | grep -v -P 'Code: (60|218)'
        sleep 0.0$RANDOM
    done
}

function thread_insert {
    while true; do
        $CLICKHOUSE_CLIENT --query "INSERT INTO $1 SELECT rand64(1), [toString(rand64(2))] FROM numbers($2)" 2>&1 | grep -v -e 'Received exception from server' -e '^(query: ' | grep -v -P 'Code: (60|218)'
        sleep 0.0$RANDOM
    done
}

function thread_insert_select {
    while true; do
        $CLICKHOUSE_CLIENT --query "INSERT INTO $1 SELECT * FROM $2" 2>&1 | grep -v -e 'Received exception from server' -e '^(query: ' | grep -v -P 'Code: (60|218)'
        sleep 0.0$RANDOM
    done
}

export -f thread_create
export -f thread_drop
export -f thread_rename
export -f thread_select
export -f thread_insert
export -f thread_insert_select


# Do randomized queries and expect nothing extraordinary happens.

function test_with_engine {
    echo "Testing $1"

    timeout 10 bash -c "thread_create t1 $1" &
    timeout 10 bash -c "thread_create t2 $1" &
    timeout 10 bash -c 'thread_drop t1' &
    timeout 10 bash -c 'thread_drop t2' &
    timeout 10 bash -c 'thread_rename t1 t2' &
    timeout 10 bash -c 'thread_rename t2 t1' &
    timeout 10 bash -c 'thread_select t1' &
    timeout 10 bash -c 'thread_select t2' &
    timeout 10 bash -c 'thread_insert t1 5' &
    timeout 10 bash -c 'thread_insert t2 10' &
    timeout 10 bash -c 'thread_insert_select t1 t2' &
    timeout 10 bash -c 'thread_insert_select t2 t1' &

    wait
    echo "Done $1"
}

test_with_engine TinyLog
test_with_engine StripeLog
test_with_engine Log

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t1"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t2"
