#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

function query()
{
    local query_id
    if [[ $1 == --query_id ]]; then
        query_id="&query_id=$2"
        shift 2
    fi
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}$query_id" -d "$*"
}

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
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"
    query "
        WITH
            (SELECT event_time_microseconds FROM system.query_log WHERE current_database = '$CLICKHOUSE_DATABASE' AND type = 'QueryStart' AND query_id = '$buffer_insert_id') AS begin_,
            (SELECT max(event_time) FROM data_01256) AS end_
        SELECT dateDiff('seconds', begin_, end_)::UInt64
    "
}

query "drop table if exists data_01256"
query "drop table if exists buffer_01256"
query "create table data_01256 (key UInt64, event_time DateTime(6) MATERIALIZED now64(6)) Engine=Memory()"

echo "min"
query "
    create table buffer_01256 (key UInt64) Engine=Buffer($CLICKHOUSE_DATABASE, data_01256, 1,
        2, 100, /* time */
        4, 100, /* rows */
        1, 1e6  /* bytes */
    )
"
min_query_id=$(random_str 10)
query --query_id "$min_query_id" "insert into buffer_01256 select * from system.numbers limit 5"
query "select count() from data_01256"
wait_until '[[ $(query "select count() from data_01256") -eq 5 ]]'
sec=$(get_buffer_delay "$min_query_id")
[[ $sec -ge 2 ]] || echo "Buffer flushed too early, min_time=2, flushed after $sec sec"
[[ $sec -lt 100 ]] || echo "Buffer flushed too late, max_time=100, flushed after $sec sec"
query "select count() from data_01256"
query "drop table buffer_01256"

echo "max"
query "
    create table buffer_01256 (key UInt64) Engine=Buffer($CLICKHOUSE_DATABASE, data_01256, 1,
        100, 2,   /* time */
        0,   100, /* rows */
        0,   1e6  /* bytes */
    )
"
max_query_id=$(random_str 10)
query --query_id "$max_query_id" "insert into buffer_01256 select * from system.numbers limit 5"
query "select count() from data_01256"
wait_until '[[ $(query "select count() from data_01256") -eq 10 ]]'
sec=$(get_buffer_delay "$max_query_id")
[[ $sec -ge 2 ]] || echo "Buffer flushed too early, max_time=2, flushed after $sec sec"
query "select count() from data_01256"
query "drop table buffer_01256"

echo "direct"
query "
    create table buffer_01256 (key UInt64) Engine=Buffer($CLICKHOUSE_DATABASE, data_01256, 1,
        100, 100, /* time */
        0,   9,   /* rows */
        0,   1e6  /* bytes */
    )
"
query "insert into buffer_01256 select * from system.numbers limit 10"
query "select count() from data_01256"

echo "drop"
query "insert into buffer_01256 select * from system.numbers limit 10"
query "drop table if exists buffer_01256"
query "select count() from data_01256"

query "drop table data_01256"
