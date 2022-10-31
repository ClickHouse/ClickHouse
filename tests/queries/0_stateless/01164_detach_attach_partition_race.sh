#!/usr/bin/env bash
# Tags: race

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "create table mt (n int) engine=MergeTree order by n"
$CLICKHOUSE_CLIENT -q "insert into mt values (1)"
$CLICKHOUSE_CLIENT -q "insert into mt values (2)"
$CLICKHOUSE_CLIENT -q "insert into mt values (3)"

function thread_insert()
{
    while true; do
        $CLICKHOUSE_CLIENT -q "insert into mt values (rand())";
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
        $CLICKHOUSE_CLIENT --allow_drop_detached -q "alter table mt drop detached partition id 'all'";
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
