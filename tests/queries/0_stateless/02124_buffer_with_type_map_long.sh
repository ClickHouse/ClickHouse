#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_buffer_map"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_buffer_map(m1 Map(String, UInt64), m2 Map(String, String)) ENGINE = Buffer('', '', 1, 1, 1, 1000000000000, 1000000000000, 1000000000000, 1000000000000)"

function insert1
{
    $CLICKHOUSE_CLIENT -q "INSERT INTO t_buffer_map SELECT (range(10), range(10)), (range(10), range(10)) from numbers(100)"
}

function select1
{
    $CLICKHOUSE_CLIENT -q "SELECT * FROM t_buffer_map" 2> /dev/null > /dev/null
}

TIMEOUT=10

export -f insert1
export -f select1

clickhouse_client_loop_timeout $TIMEOUT insert1 &
clickhouse_client_loop_timeout $TIMEOUT select1 &

wait

echo "OK"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_buffer_map"
