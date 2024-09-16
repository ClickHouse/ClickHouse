#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function elapsed_sec()
{
    local expr=$1 && shift
    local start end
    start=$(date +%s.%N)
    while ! eval "$expr"; do
        sleep 0.5
    done
    end=$(date +%s.%N)
    $CLICKHOUSE_LOCAL -q "select floor($end-$start)"
}

$CLICKHOUSE_CLIENT -nm -q "
    drop table if exists data_01256;
    drop table if exists buffer_01256;

    create table data_01256 as system.numbers Engine=Memory();
"

echo "min"
$CLICKHOUSE_CLIENT -nm -q "
    create table buffer_01256 as system.numbers Engine=Buffer(currentDatabase(), data_01256, 1,
        2, 100, /* time */
        4, 100, /* rows */
        1, 1e6  /* bytes */
    );
    insert into buffer_01256 select * from system.numbers limit 5;
    select count() from data_01256;
"
sec=$(elapsed_sec '[[ $($CLICKHOUSE_CLIENT -q "select count() from data_01256") -eq 5 ]]')
[[ $sec -ge 2 ]] || echo "Buffer flushed too early, min_time=2, flushed after $sec sec"
[[ $sec -lt 100 ]] || echo "Buffer flushed too late, max_time=100, flushed after $sec sec"
$CLICKHOUSE_CLIENT -q "select count() from data_01256"
$CLICKHOUSE_CLIENT -q "drop table buffer_01256"

echo "max"
$CLICKHOUSE_CLIENT -nm -q "
    create table buffer_01256 as system.numbers Engine=Buffer(currentDatabase(), data_01256, 1,
        100, 2,   /* time */
        0,   100, /* rows */
        0,   1e6  /* bytes */
    );
    insert into buffer_01256 select * from system.numbers limit 5;
    select count() from data_01256;
"
sec=$(elapsed_sec '[[ $($CLICKHOUSE_CLIENT -q "select count() from data_01256") -eq 10 ]]')
[[ $sec -ge 2 ]] || echo "Buffer flushed too early, max_time=2, flushed after $sec sec"
$CLICKHOUSE_CLIENT -q "select count() from data_01256"
$CLICKHOUSE_CLIENT -q "drop table buffer_01256"

echo "direct"
$CLICKHOUSE_CLIENT -nm -q "
    create table buffer_01256 as system.numbers Engine=Buffer(currentDatabase(), data_01256, 1,
        100, 100, /* time */
        0,   9,   /* rows */
        0,   1e6  /* bytes */
    );
    insert into buffer_01256 select * from system.numbers limit 10;
    select count() from data_01256;
"

echo "drop"
$CLICKHOUSE_CLIENT -nm -q "
    insert into buffer_01256 select * from system.numbers limit 10;
    drop table if exists buffer_01256;
    select count() from data_01256;
"

$CLICKHOUSE_CLIENT -q "drop table data_01256"
