#!/usr/bin/env bash
# Tags: race, no-parallel

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS mem"
$CLICKHOUSE_CLIENT -q "CREATE TABLE mem (x UInt64) engine = Memory"

function f()
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM (SELECT * FROM mem SETTINGS max_threads=2) FORMAT Null;"
}

function g()
{
    $CLICKHOUSE_CLIENT -n -q "
        INSERT INTO mem SELECT number FROM numbers(1000000);
        INSERT INTO mem SELECT number FROM numbers(1000000);
        INSERT INTO mem SELECT number FROM numbers(1000000);
        INSERT INTO mem VALUES (1);
        INSERT INTO mem VALUES (1);
        INSERT INTO mem VALUES (1);
        INSERT INTO mem VALUES (1);
        INSERT INTO mem VALUES (1);
        INSERT INTO mem VALUES (1);
        TRUNCATE TABLE mem;
    "
}

export -f f
export -f g

clickhouse_client_loop_timeout 30 f > /dev/null &
clickhouse_client_loop_timeout 30 g > /dev/null &
wait

$CLICKHOUSE_CLIENT -q "DROP TABLE mem"
