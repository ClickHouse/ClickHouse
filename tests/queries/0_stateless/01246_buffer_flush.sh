#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

function wait_until()
{
    local expr=$1 && shift
    while ! eval "$expr"; do
        sleep 0.5
    done
}
function get_buffer_delay()
{
    local buffer_insert_id=$1 && shift
    $CLICKHOUSE_CLIENT -nm -q "
    SYSTEM FLUSH LOGS;
    WITH
        (SELECT event_time_microseconds FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryStart' AND query_id = '$buffer_insert_id') AS begin_,
        (SELECT max(event_time) FROM data_01256) AS end_
    SELECT dateDiff('seconds', begin_, end_)::UInt64;
    "
}

$CLICKHOUSE_CLIENT -nm -q "
    drop table if exists data_01256;
    drop table if exists buffer_01256;

    create table data_01256 (key UInt64, event_time DateTime(6) MATERIALIZED now64(6)) Engine=Memory();
"

echo "min"
$CLICKHOUSE_CLIENT -q "
    create table buffer_01256 (key UInt64) Engine=Buffer(currentDatabase(), data_01256, 1,
        2, 100, /* time */
        4, 100, /* rows */
        1, 1e6  /* bytes */
    )
"
min_query_id=$(random_str 10)
$CLICKHOUSE_CLIENT --query_id="$min_query_id" -q "insert into buffer_01256 select * from system.numbers limit 5"
$CLICKHOUSE_CLIENT -q "select count() from data_01256"
wait_until '[[ $($CLICKHOUSE_CLIENT -q "select count() from data_01256") -eq 5 ]]'
sec=$(get_buffer_delay "$min_query_id")
[[ $sec -ge 2 ]] || echo "Buffer flushed too early, min_time=2, flushed after $sec sec"
[[ $sec -lt 100 ]] || echo "Buffer flushed too late, max_time=100, flushed after $sec sec"
$CLICKHOUSE_CLIENT -q "select count() from data_01256"
$CLICKHOUSE_CLIENT -q "drop table buffer_01256"

echo "max"
$CLICKHOUSE_CLIENT -q "
    create table buffer_01256 (key UInt64) Engine=Buffer(currentDatabase(), data_01256, 1,
        100, 2,   /* time */
        0,   100, /* rows */
        0,   1e6  /* bytes */
    );
"
max_query_id=$(random_str 10)
$CLICKHOUSE_CLIENT --query_id="$max_query_id" -q "insert into buffer_01256 select * from system.numbers limit 5"
$CLICKHOUSE_CLIENT -q "select count() from data_01256"
wait_until '[[ $($CLICKHOUSE_CLIENT -q "select count() from data_01256") -eq 10 ]]'
sec=$(get_buffer_delay "$max_query_id")
[[ $sec -ge 2 ]] || echo "Buffer flushed too early, max_time=2, flushed after $sec sec"
$CLICKHOUSE_CLIENT -q "select count() from data_01256"
$CLICKHOUSE_CLIENT -q "drop table buffer_01256"

echo "direct"
$CLICKHOUSE_CLIENT -nm -q "
    create table buffer_01256 (key UInt64) Engine=Buffer(currentDatabase(), data_01256, 1,
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
