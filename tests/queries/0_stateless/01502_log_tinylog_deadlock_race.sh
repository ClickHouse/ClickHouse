#!/usr/bin/env bash
# Tags: deadlock

set -e

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


function thread_create()
{
    $CLICKHOUSE_CLIENT --query "CREATE TABLE IF NOT EXISTS $1 (x UInt64, s Array(Nullable(String))) ENGINE = $2"
    sleep 0.0$RANDOM
}

function thread_drop()
{
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $1"
    sleep 0.0$RANDOM
}

function thread_rename()
{
    $CLICKHOUSE_CLIENT --query "RENAME TABLE $1 TO $2" 2>&1 | grep -v -e 'Received exception from server' -e '^(query: ' | grep -v -P 'Code: (60|57)'
    sleep 0.0$RANDOM
}

function thread_select()
{
    $CLICKHOUSE_CLIENT --query "SELECT * FROM $1 FORMAT Null" 2>&1 | grep -v -e 'Received exception from server' -e '^(query: ' | grep -v -P 'Code: (60|218)'
    sleep 0.0$RANDOM
}

function thread_insert()
{
    $CLICKHOUSE_CLIENT --query "INSERT INTO $1 SELECT rand64(1), [toString(rand64(2))] FROM numbers($2)" 2>&1 | grep -v -e 'Received exception from server' -e '^(query: '| grep -v -P 'Code: (60|218)'
    sleep 0.0$RANDOM
}

function thread_insert_select()
{
    $CLICKHOUSE_CLIENT --query "INSERT INTO $1 SELECT * FROM $2" 2>&1 | grep -v -e 'Received exception from server' -e '^(query: ' | grep -v -P 'Code: (60|218)'
    sleep 0.0$RANDOM
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

    clickhouse_client_loop_timeout 10 thread_create t1 $1 &
    clickhouse_client_loop_timeout 10 thread_create t2 $1 &
    clickhouse_client_loop_timeout 10 thread_drop t1 &
    clickhouse_client_loop_timeout 10 thread_drop t2 &
    clickhouse_client_loop_timeout 10 thread_rename t1 t2 &
    clickhouse_client_loop_timeout 10 thread_rename t2 t1 &
    clickhouse_client_loop_timeout 10 thread_select t1 &
    clickhouse_client_loop_timeout 10 thread_select t2 &
    clickhouse_client_loop_timeout 10 thread_insert t1 5 &
    clickhouse_client_loop_timeout 10 thread_insert t2 10 &
    clickhouse_client_loop_timeout 10 thread_insert_select t1 t2 &
    clickhouse_client_loop_timeout 10 thread_insert_select t2 t1 &

    wait
    echo "Done $1"
}

#test_with_engine TinyLog
#test_with_engine StripeLog
#test_with_engine Log
test_with_engine Memory

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t1"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t2"
