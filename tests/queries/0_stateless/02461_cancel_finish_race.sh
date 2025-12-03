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

# https://stackoverflow.com/questions/9954794/execute-a-shell-function-with-timeout
export -f thread_query;
export -f thread_cancel;

TIMEOUT=30

timeout $TIMEOUT bash -c thread_query 2> /dev/null &
timeout $TIMEOUT bash -c thread_cancel 2> /dev/null &

timeout $TIMEOUT bash -c thread_query 2> /dev/null &
timeout $TIMEOUT bash -c thread_cancel 2> /dev/null &

timeout $TIMEOUT bash -c thread_query 2> /dev/null &
timeout $TIMEOUT bash -c thread_cancel 2> /dev/null &

timeout $TIMEOUT bash -c thread_query 2> /dev/null &
timeout $TIMEOUT bash -c thread_cancel 2> /dev/null &

timeout $TIMEOUT bash -c thread_query 2> /dev/null &
timeout $TIMEOUT bash -c thread_cancel 2> /dev/null &

timeout $TIMEOUT bash -c thread_query 2> /dev/null &
timeout $TIMEOUT bash -c thread_cancel 2> /dev/null &

timeout $TIMEOUT bash -c thread_query 2> /dev/null &
timeout $TIMEOUT bash -c thread_cancel 2> /dev/null &

timeout $TIMEOUT bash -c thread_query 2> /dev/null &
timeout $TIMEOUT bash -c thread_cancel 2> /dev/null &

timeout $TIMEOUT bash -c thread_query 2> /dev/null &
timeout $TIMEOUT bash -c thread_cancel 2> /dev/null &

timeout $TIMEOUT bash -c thread_query 2> /dev/null &
timeout $TIMEOUT bash -c thread_cancel 2> /dev/null &

wait
