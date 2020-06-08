#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

export CURR_DATABASE="test_lazy_01294_concurrent_${CLICKHOUSE_DATABASE}"


function recreate_lazy_func1()
{
    $CLICKHOUSE_CLIENT -q "
        CREATE TABLE $CURR_DATABASE.log (a UInt64, b UInt64) ENGINE = Log;
    ";

    while true; do
        $CLICKHOUSE_CLIENT -q "
            DETACH TABLE $CURR_DATABASE.log;
        ";

        $CLICKHOUSE_CLIENT -q "
            ATTACH TABLE $CURR_DATABASE.log;
        ";
        done
}

function recreate_lazy_func2()
{
    while true; do
        $CLICKHOUSE_CLIENT -q "
            CREATE TABLE $CURR_DATABASE.tlog (a UInt64, b UInt64) ENGINE = TinyLog;
        ";

        $CLICKHOUSE_CLIENT -q "
            DROP TABLE $CURR_DATABASE.tlog;
        ";
        done
}

function recreate_lazy_func3()
{
    $CLICKHOUSE_CLIENT -q "
        CREATE TABLE $CURR_DATABASE.slog (a UInt64, b UInt64) ENGINE = StripeLog;
    ";

    while true; do
        $CLICKHOUSE_CLIENT -q "
            ATTACH TABLE $CURR_DATABASE.slog;
        ";

        $CLICKHOUSE_CLIENT -q "
            DETACH TABLE $CURR_DATABASE.slog;
        ";
        done
}

function recreate_lazy_func4()
{
    while true; do
        $CLICKHOUSE_CLIENT -q "
            CREATE TABLE $CURR_DATABASE.tlog2 (a UInt64, b UInt64) ENGINE = TinyLog;
        ";

        $CLICKHOUSE_CLIENT -q "
            DROP TABLE $CURR_DATABASE.tlog2;
        ";
        done
}

function test_func()
{
    while true; do
        $CLICKHOUSE_CLIENT -q "SYSTEM STOP TTL MERGES";
        done
}


export -f recreate_lazy_func1;
export -f recreate_lazy_func2;
export -f recreate_lazy_func3;
export -f recreate_lazy_func4;
export -f test_func;


${CLICKHOUSE_CLIENT} -n -q "
    DROP DATABASE IF EXISTS $CURR_DATABASE;
    CREATE DATABASE $CURR_DATABASE ENGINE = Lazy(1);
"


TIMEOUT=30

timeout $TIMEOUT bash -c recreate_lazy_func1 2> /dev/null &
timeout $TIMEOUT bash -c recreate_lazy_func2 2> /dev/null &
timeout $TIMEOUT bash -c recreate_lazy_func3 2> /dev/null &
timeout $TIMEOUT bash -c recreate_lazy_func4 2> /dev/null &
timeout $TIMEOUT bash -c test_func 2> /dev/null &

wait
sleep 1

${CLICKHOUSE_CLIENT} -n -q "
    DROP TABLE IF EXISTS $CURR_DATABASE.log;
    DROP TABLE IF EXISTS $CURR_DATABASE.slog;
    DROP TABLE IF EXISTS $CURR_DATABASE.tlog;
    DROP TABLE IF EXISTS $CURR_DATABASE.tlog2;
"

$CLICKHOUSE_CLIENT -q "SYSTEM START TTL MERGES";
echo "Test OK"
