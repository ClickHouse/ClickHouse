#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function wait_with_limit()
{
    local limit=$1 && shift
    local expr=$1 && shift

    for ((i = 0; i < limit; ++i)); do
        if eval "$expr"; then
            break
        fi
        sleep 1
    done
}

$CLICKHOUSE_CLIENT -nm -q "
    drop table if exists data_01811;
    drop table if exists buffer_01811;


    create table data_01811 (key Int) Engine=Memory();
    /* Buffer with flush_rows=1000 */
    create table buffer_01811 (key Int) Engine=Buffer(currentDatabase(), data_01811,
        /* num_layers= */ 1,
        /* min_time= */   1,     /* max_time= */  86400,
        /* min_rows= */   1e9,   /* max_rows= */  1e6,
        /* min_bytes= */  0,     /* max_bytes= */ 4e6,
        /* flush_time= */ 86400, /* flush_rows= */ 10, /* flush_bytes= */0
    );

    insert into buffer_01811 select * from numbers(10);
    insert into buffer_01811 select * from numbers(10);
"

# wait for background buffer flush
wait_with_limit 30 '[[ $($CLICKHOUSE_CLIENT -q "select count() from data_01811") -gt 0 ]]'

$CLICKHOUSE_CLIENT -nm -q "select count() from data_01811"

$CLICKHOUSE_CLIENT -nm -q "
    drop table buffer_01811;
    drop table data_01811;
"
