#!/usr/bin/env bash
# Tags: deadlock, no-parallel, log-engine

set -e

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


function thread_create {
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query "CREATE TABLE IF NOT EXISTS $1 (x UInt64, s Array(Nullable(String))) ENGINE = $2" 2>&1 | grep -v -F 'Received exception from server' | grep -v -P 'Code: (60|57|741)'
        sleep 0.0$RANDOM
    done
}

function thread_drop {
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $1" 2>&1 | grep -v -e 'Received exception from server' -e '^(query: ' | grep -v -P 'Code: (60|57|741)'
        sleep 0.0$RANDOM
    done
}

function thread_rename {
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query "RENAME TABLE $1 TO $2" 2>&1 | grep -v -e 'Received exception from server' -e '^(query: ' | grep -v -P 'Code: (60|57|741)'
        sleep 0.0$RANDOM
    done
}

function thread_select {
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --local_filesystem_read_method pread --query "SELECT * FROM $1 FORMAT Null" 2>&1 | grep -v -e 'Received exception from server' -e '^(query: ' | grep -v -P 'Code: (60|218|741)'
        sleep 0.0$RANDOM
    done
}

function thread_insert {
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query "INSERT INTO $1 SELECT rand64(1), [toString(rand64(2))] FROM numbers($2)" 2>&1 | grep -v -e 'Received exception from server' -e '^(query: ' | grep -v -P 'Code: (60|218|741)'
        sleep 0.0$RANDOM
    done
}

function thread_insert_select {
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --local_filesystem_read_method pread --query "INSERT INTO $1 SELECT * FROM $2" 2>&1 | grep -v -e 'Received exception from server' -e '^(query: ' | grep -v -P 'Code: (60|218|741)'
        sleep 0.0$RANDOM
    done
}

# Do randomized queries and expect nothing extraordinary happens.
TIMEOUT=10

function test_with_engine {
    echo "Testing $1"

    thread_create t1 $1 &
    thread_create t2 $1 &
    thread_drop t1 &
    thread_drop t2 &
    thread_rename t1 t2 &
    thread_rename t2 t1 &
    thread_select t1 &
    thread_select t2 &
    thread_insert t1 5 &
    thread_insert t2 10 &
    thread_insert_select t1 t2 &
    thread_insert_select t2 t1 &

    wait
    echo "Done $1"
}

test_with_engine TinyLog
test_with_engine StripeLog
test_with_engine Log

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t1"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t2"

# It is not enough to kill the commands running the queries, we also have to kill the queries, the server might be still running
# to avoid the following error:
# Code: 219. DB::Exception: New table appeared in database being dropped or detached. Try again. (DATABASE_NOT_EMPTY)

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE current_database = currentDatabase() SYNC FORMAT Null"
