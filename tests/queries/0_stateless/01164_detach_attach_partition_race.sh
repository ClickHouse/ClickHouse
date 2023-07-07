#!/usr/bin/env bash
# Tags: race

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists mt"

$CLICKHOUSE_CLIENT -q "create table mt (n int) engine=MergeTree order by n settings parts_to_throw_insert=1000"
$CLICKHOUSE_CLIENT -q "insert into mt values (1)"
$CLICKHOUSE_CLIENT -q "insert into mt values (2)"
$CLICKHOUSE_CLIENT -q "insert into mt values (3)"

function thread_insert()
{
    while true; do
        # It might be the case that the threads are terminated and exited, but some children didn't and they are still sending queries when we are dropping tables.
        # That's why the "Table doesn't exist" error is allowed, while other errors don't.
        $CLICKHOUSE_CLIENT -q "insert into mt values (rand())" 2>&1 | tr -d '\n' | rg -v "Table .+ doesn't exist";
    done
}

function thread_detach_attach()
{
    while true; do
        $CLICKHOUSE_CLIENT -q "alter table mt detach partition id 'all'";
        $CLICKHOUSE_CLIENT -q "alter table mt attach partition id 'all'";
    done
}

function thread_drop_detached()
{
    while true; do
        $CLICKHOUSE_CLIENT --allow_drop_detached 1 -q "alter table mt drop detached partition id 'all'";
    done
}

export -f thread_insert;
export -f thread_detach_attach;
export -f thread_drop_detached;

TIMEOUT=10

timeout $TIMEOUT bash -c thread_insert &
timeout $TIMEOUT bash -c thread_detach_attach 2> /dev/null &
timeout $TIMEOUT bash -c thread_detach_attach 2> /dev/null &
timeout $TIMEOUT bash -c thread_drop_detached 2> /dev/null &

wait

$CLICKHOUSE_CLIENT -q "drop table mt"
