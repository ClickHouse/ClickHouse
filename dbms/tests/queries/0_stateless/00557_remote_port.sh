#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

if [ $CLICKHOUSE_HOST == "localhost" ]; then
    $CLICKHOUSE_CLIENT -q "SELECT * FROM remote('127.0.0.2',          system, one);"
    $CLICKHOUSE_CLIENT -q "SELECT * FROM remote('127.0.0.{2,2}',      system, one);"
    $CLICKHOUSE_CLIENT -q "SELECT * FROM remote('127.0.0.2:${CLICKHOUSE_PORT_TCP}',     system, one);"
    $CLICKHOUSE_CLIENT -q "SELECT * FROM remote('127.0.0.{2,2}:${CLICKHOUSE_PORT_TCP}', system, one);"
else
    # Can't test without localhost
    echo 0
    echo 0
    echo 0
    echo 0
    echo 0
    echo 0
fi

$CLICKHOUSE_CLIENT -q "SELECT * FROM remote('${CLICKHOUSE_HOST}',          system, one);"
$CLICKHOUSE_CLIENT -q "SELECT * FROM remote('${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_TCP}',     system, one);"
