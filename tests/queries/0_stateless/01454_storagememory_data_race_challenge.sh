#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS mem"
$CLICKHOUSE_CLIENT -q "CREATE TABLE mem (x UInt64) engine = Memory"

function f {
  for _ in $(seq 1 300); do
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM (SELECT * FROM mem SETTINGS max_threads=2) FORMAT Null;"
  done
}

function g {
  for _ in $(seq 1 100); do
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
  done
}

export -f f;
export -f g;

timeout 30 bash -c f > /dev/null &
timeout 30 bash -c g > /dev/null &
wait

$CLICKHOUSE_CLIENT -q "DROP TABLE mem"
