#!/usr/bin/env bash
# Tags: race

unset CLICKHOUSE_LOG_COMMENT

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -o errexit
set -o pipefail

echo "
    DROP TABLE IF EXISTS storage_join_race;
    CREATE TABLE storage_join_race (x UInt64, y UInt64) Engine = Join(ALL, FULL, x);
" | $CLICKHOUSE_CLIENT -n

function read_thread_big()
{
    $CLICKHOUSE_CLIENT -n -q "SELECT * FROM ( SELECT number AS x FROM numbers(100000) ) AS t1 ALL FULL JOIN storage_join_race USING (x) FORMAT Null"
}

function read_thread_small()
{
    $CLICKHOUSE_CLIENT -n -q "SELECT * FROM ( SELECT number AS x FROM numbers(10) ) AS t1 ALL FULL JOIN storage_join_race USING (x) FORMAT Null"
}

function read_thread_select()
{
    $CLICKHOUSE_CLIENT -n -q "SELECT * FROM storage_join_race FORMAT Null"
}

export -f read_thread_big
export -f read_thread_small
export -f read_thread_select

TIMEOUT=20

clickhouse_client_loop_timeout $TIMEOUT read_thread_big 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT read_thread_small 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT read_thread_select 2> /dev/null &

echo "
    INSERT INTO storage_join_race SELECT number AS x, number AS y FROM numbers (10000000);
" | $CLICKHOUSE_CLIENT -n

wait

$CLICKHOUSE_CLIENT -q "DROP TABLE storage_join_race"
