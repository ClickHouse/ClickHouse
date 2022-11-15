#!/usr/bin/env bash
# Tags: race, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

function thread1()
{
    while true; do 
        $CLICKHOUSE_CLIENT --query_id=hello_01003 --query "SELECT count() FROM numbers(1000000000)" --format Null;
    done
}

function thread2()
{
    while true; do
        $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = 'hello_01003'" --format Null;
        sleep 0.$RANDOM
    done
}

function thread3()
{
    while true; do
        $CLICKHOUSE_CLIENT --query "SHOW PROCESSLIST" --format Null;
        $CLICKHOUSE_CLIENT --query "SELECT * FROM system.processes" --format Null;
    done
}

# https://stackoverflow.com/questions/9954794/execute-a-shell-function-with-timeout
export -f thread1;
export -f thread2;
export -f thread3;

TIMEOUT=10

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread1 2> /dev/null &

timeout $TIMEOUT bash -c thread2 2> /dev/null &

timeout $TIMEOUT bash -c thread3 2> /dev/null &
timeout $TIMEOUT bash -c thread3 2> /dev/null &
timeout $TIMEOUT bash -c thread3 2> /dev/null &
timeout $TIMEOUT bash -c thread3 2> /dev/null &

wait
