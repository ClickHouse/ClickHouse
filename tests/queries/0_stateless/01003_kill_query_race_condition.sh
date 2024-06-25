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


TIMEOUT=10

spawn_with_timeout $TIMEOUT thread1 2> /dev/null
spawn_with_timeout $TIMEOUT thread1 2> /dev/null
spawn_with_timeout $TIMEOUT thread1 2> /dev/null
spawn_with_timeout $TIMEOUT thread1 2> /dev/null

spawn_with_timeout $TIMEOUT thread2 2> /dev/null

spawn_with_timeout $TIMEOUT thread3 2> /dev/null
spawn_with_timeout $TIMEOUT thread3 2> /dev/null
spawn_with_timeout $TIMEOUT thread3 2> /dev/null
spawn_with_timeout $TIMEOUT thread3 2> /dev/null

wait
