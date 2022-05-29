#!/usr/bin/env bash
# Tags: race, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

function thread1()
{
    $CLICKHOUSE_CLIENT --query_id=hello_01003 --query "SELECT count() FROM numbers(1000000000)" --format Null;
}

function thread2()
{
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = 'hello_01003'" --format Null
    sleep 0.$RANDOM
}

function thread3()
{
    $CLICKHOUSE_CLIENT --query "SHOW PROCESSLIST" --format Null
    $CLICKHOUSE_CLIENT --query "SELECT * FROM system.processes" --format Null
}

export -f thread1
export -f thread2
export -f thread3

TIMEOUT=10

clickhouse_client_loop_timeout $TIMEOUT thread1 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread1 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread1 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread1 2> /dev/null &

clickhouse_client_loop_timeout $TIMEOUT thread2 2> /dev/null &

clickhouse_client_loop_timeout $TIMEOUT thread3 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread3 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread3 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread3 2> /dev/null &

wait
