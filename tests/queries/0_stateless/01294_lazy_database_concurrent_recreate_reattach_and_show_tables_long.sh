#!/usr/bin/env bash
# Tags: long, no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

export CURR_DATABASE="test_lazy_01294_concurrent_${CLICKHOUSE_DATABASE}"


function recreate_lazy_func1()
{
    $CLICKHOUSE_CLIENT -nm -q "
        DETACH TABLE $CURR_DATABASE.log;
        ATTACH TABLE $CURR_DATABASE.log;
    ";
}

function recreate_lazy_func2()
{
    $CLICKHOUSE_CLIENT -nm -q "
        CREATE TABLE $CURR_DATABASE.tlog (a UInt64, b UInt64) ENGINE = TinyLog;
        DROP TABLE $CURR_DATABASE.tlog;
    "
}

function recreate_lazy_func3()
{
    $CLICKHOUSE_CLIENT -nm -q "
        ATTACH TABLE $CURR_DATABASE.slog;
        DETACH TABLE $CURR_DATABASE.slog;
    "
}

function recreate_lazy_func4()
{
    $CLICKHOUSE_CLIENT -nm -q "
        CREATE TABLE $CURR_DATABASE.tlog2 (a UInt64, b UInt64) ENGINE = TinyLog;
        DROP TABLE $CURR_DATABASE.tlog2;
    "
}

function test_func()
{
    for table in log tlog slog tlog2; do
        $CLICKHOUSE_CLIENT -q "SYSTEM STOP TTL MERGES $CURR_DATABASE.$table" >& /dev/null
    done
}


export -f recreate_lazy_func1
export -f recreate_lazy_func2
export -f recreate_lazy_func3
export -f recreate_lazy_func4
export -f test_func


${CLICKHOUSE_CLIENT} -n -q "
    DROP DATABASE IF EXISTS $CURR_DATABASE;
    CREATE DATABASE $CURR_DATABASE ENGINE = Lazy(1);

    CREATE TABLE $CURR_DATABASE.log (a UInt64, b UInt64) ENGINE = Log;
    CREATE TABLE $CURR_DATABASE.slog (a UInt64, b UInt64) ENGINE = StripeLog;
"


TIMEOUT=30

clickhouse_client_loop_timeout $TIMEOUT recreate_lazy_func1 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT recreate_lazy_func2 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT recreate_lazy_func3 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT recreate_lazy_func4 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT test_func 2> /dev/null &

wait
sleep 1

for table in log tlog slog tlog2; do
    $CLICKHOUSE_CLIENT -q "SYSTEM STOP TTL MERGES $CURR_DATABASE.$table" >& /dev/null
  ${CLICKHOUSE_CLIENT} -q "ATTACH TABLE $CURR_DATABASE.$table;" 2>/dev/null
done

${CLICKHOUSE_CLIENT} -q "DROP DATABASE $CURR_DATABASE"

echo "Test OK"

