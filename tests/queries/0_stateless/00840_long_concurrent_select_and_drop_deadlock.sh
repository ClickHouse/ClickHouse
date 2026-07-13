#!/usr/bin/env bash
# Tags: deadlock, no-debug, no-parallel

# NOTE: database = $CLICKHOUSE_DATABASE is unwanted

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function cleanup()
{
    echo Failed
    wait
}

trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "create view view_00840 as select count(*),database,table from system.columns group by database,table"


function thread_drop_create()
{
    local TIMELIMIT=$((SECONDS+$1))
    local it=0
    while [ $SECONDS -lt "$TIMELIMIT" ] && [ $it -lt 100 ];
    do
        it=$((it+1))
        $CLICKHOUSE_CLIENT -m -q "
            drop table if exists view_00840;
            create view view_00840 as select count(*),database,table from system.columns group by database,table;
        "
    done
}

function thread_select()
{
    local TIMELIMIT=$((SECONDS+$1))
    local it=0
    while [ $SECONDS -lt "$TIMELIMIT" ] && [ $it -lt 250 ];
    do
        it=$((it+1))
        $CLICKHOUSE_CLIENT -q "select * from view_00840 order by table" >/dev/null 2>&1 || true
    done
}


export -f thread_drop_create
export -f thread_select

TIMEOUT=30
thread_drop_create $TIMEOUT &
thread_select $TIMEOUT &

wait
trap '' EXIT

echo "drop table view_00840" | $CLICKHOUSE_CLIENT

echo 'did not deadlock'
