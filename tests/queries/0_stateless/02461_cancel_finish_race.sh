#!/usr/bin/env bash
# Tags: no-fasttest


CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function thread_query()
{
    while true; do
        $CLICKHOUSE_CLIENT --query "SELECT count() FROM numbers_mt(10000) WHERE rand() = 0 FORMAT Null";
    done
}

function thread_cancel()
{
    while true; do
        $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE current_database = '$CLICKHOUSE_DATABASE' SYNC FORMAT Null";
    done
}


TIMEOUT=30

spawn_with_timeout $TIMEOUT thread_query 2> /dev/null
spawn_with_timeout $TIMEOUT thread_cancel 2> /dev/null

spawn_with_timeout $TIMEOUT thread_query 2> /dev/null
spawn_with_timeout $TIMEOUT thread_cancel 2> /dev/null

spawn_with_timeout $TIMEOUT thread_query 2> /dev/null
spawn_with_timeout $TIMEOUT thread_cancel 2> /dev/null

spawn_with_timeout $TIMEOUT thread_query 2> /dev/null
spawn_with_timeout $TIMEOUT thread_cancel 2> /dev/null

spawn_with_timeout $TIMEOUT thread_query 2> /dev/null
spawn_with_timeout $TIMEOUT thread_cancel 2> /dev/null

spawn_with_timeout $TIMEOUT thread_query 2> /dev/null
spawn_with_timeout $TIMEOUT thread_cancel 2> /dev/null

spawn_with_timeout $TIMEOUT thread_query 2> /dev/null
spawn_with_timeout $TIMEOUT thread_cancel 2> /dev/null

spawn_with_timeout $TIMEOUT thread_query 2> /dev/null
spawn_with_timeout $TIMEOUT thread_cancel 2> /dev/null

spawn_with_timeout $TIMEOUT thread_query 2> /dev/null
spawn_with_timeout $TIMEOUT thread_cancel 2> /dev/null

spawn_with_timeout $TIMEOUT thread_query 2> /dev/null
spawn_with_timeout $TIMEOUT thread_cancel 2> /dev/null

wait
