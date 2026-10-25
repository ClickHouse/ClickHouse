#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

function query()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "$*"
}

function wait_until()
{
    local expr=$1 && shift
    while ! eval "$expr"; do
        sleep 0.5
    done
}
# insert_time is filled by the INSERT into the Buffer, flush_time by the
# flush into the destination; the latest of each belongs to the last section.
function get_buffer_delay()
{
    query "select age('second', max(insert_time), max(flush_time)) from data_01256"
}

query "drop table if exists data_01256"
query "drop table if exists buffer_01256"
query "create table data_01256 (key UInt64, insert_time DateTime64(6), flush_time DateTime64(6) MATERIALIZED now64(6)) Engine=Memory()"

echo "min"
query "
    create table buffer_01256 (key UInt64, insert_time DateTime64(6)) Engine=Buffer($CLICKHOUSE_DATABASE, data_01256, 1,
        2, 100, /* time */
        4, 100, /* rows */
        1, 1e6  /* bytes */
    )
"
query "insert into buffer_01256 select number, now64(6) from numbers(5)"
wait_until '[[ $(query "select count() from data_01256") -eq 5 ]]'
sec=$(get_buffer_delay)
[[ $sec -ge 2 ]] || echo "Buffer flushed too early, min_time=2, flushed after $sec sec"
[[ $sec -lt 10 ]] || echo "Buffer flushed too late, min_time=2, flushed after $sec sec"
query "select count() from data_01256"
query "drop table buffer_01256"

echo "max"
query "
    create table buffer_01256 (key UInt64, insert_time DateTime64(6)) Engine=Buffer($CLICKHOUSE_DATABASE, data_01256, 1,
        100, 2,   /* time */
        0,   100, /* rows */
        0,   1e6  /* bytes */
    )
"
query "insert into buffer_01256 select number, now64(6) from numbers(5)"
wait_until '[[ $(query "select count() from data_01256") -eq 10 ]]'
sec=$(get_buffer_delay)
[[ $sec -ge 2 ]] || echo "Buffer flushed too early, max_time=2, flushed after $sec sec"
[[ $sec -lt 10 ]] || echo "Buffer flushed too late, max_time=2, flushed after $sec sec"
query "select count() from data_01256"
query "drop table buffer_01256"

echo "direct"
query "
    create table buffer_01256 (key UInt64, insert_time DateTime64(6)) Engine=Buffer($CLICKHOUSE_DATABASE, data_01256, 1,
        100, 100, /* time */
        0,   9,   /* rows */
        0,   1e6  /* bytes */
    )
"
query "insert into buffer_01256 select number, now64(6) from numbers(10)"
query "select count() from data_01256"

echo "drop"
query "insert into buffer_01256 select number, now64(6) from numbers(10)"
query "drop table if exists buffer_01256"
query "select count() from data_01256"

query "drop table data_01256"
