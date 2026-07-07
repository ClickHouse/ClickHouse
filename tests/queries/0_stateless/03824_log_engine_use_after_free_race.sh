#!/usr/bin/env bash
# Tags: race, log-engine

# Test for use-after-free race condition in StorageLog/StorageStripeLog/TinyLog
# where LogSource holds a reference to the storage object, but the storage
# can be destroyed by a concurrent DROP TABLE while reading is still in progress.

set -e

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TIMEOUT=10

function thread_create_insert {
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query "CREATE TABLE IF NOT EXISTS $1 (x UInt64, s String) ENGINE = $2" 2>&1 | grep -v -e 'Received exception from server' -e '^(query: ' | grep -v -P 'Code: (57|60|741)'
        $CLICKHOUSE_CLIENT --query "INSERT INTO $1 SELECT number, repeat(toString(number), 100) FROM numbers(10000)" 2>&1 | grep -v -e 'Received exception from server' -e '^(query: ' | grep -v -P 'Code: (57|60|741)'
    done
}

function thread_select {
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --local_filesystem_read_method pread --query "SELECT * FROM $1 FORMAT Null" 2>&1 | grep -v -e 'Received exception from server' -e '^(query: ' | grep -v -P 'Code: (60|218|741)'
    done
}

function thread_drop {
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $1" 2>&1 | grep -v -e 'Received exception from server' -e '^(query: ' | grep -v -P 'Code: (57|60|741)'
    done
}

function test_with_engine {
    echo "Testing $1"

    thread_create_insert t_race_log $1 &
    thread_select t_race_log &
    thread_select t_race_log &
    thread_select t_race_log &
    thread_select t_race_log &
    thread_drop t_race_log &
    thread_drop t_race_log &

    wait
    echo "Done $1"
}

test_with_engine TinyLog
test_with_engine StripeLog
test_with_engine Log

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_race_log"
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE current_database = currentDatabase() SYNC FORMAT Null"
