#!/usr/bin/env bash
# Tags: race

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists mt"

$CLICKHOUSE_CLIENT -q "create table mt (n int) engine=MergeTree order by n settings parts_to_throw_insert=5000"
$CLICKHOUSE_CLIENT -q "insert into mt values (1)"
$CLICKHOUSE_CLIENT -q "insert into mt values (2)"
$CLICKHOUSE_CLIENT -q "insert into mt values (3)"

function thread_insert()
{
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT -q "insert into mt values (rand())";
    done
}

function thread_detach_attach()
{
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT -q "alter table mt detach partition id 'all'";
        $CLICKHOUSE_CLIENT -q "alter table mt attach partition id 'all'";
    done
}

function thread_drop_detached()
{
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT --allow_drop_detached 1 -q "alter table mt drop detached partition id 'all'";
    done
}

export -f thread_insert;
export -f thread_detach_attach;
export -f thread_drop_detached;

TIMEOUT=10

thread_insert $TIMEOUT &
thread_detach_attach $TIMEOUT 2> /dev/null &
thread_detach_attach $TIMEOUT 2> /dev/null &
thread_drop_detached $TIMEOUT 2> /dev/null &

wait

$CLICKHOUSE_CLIENT -q "drop table mt"
