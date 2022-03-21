#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -mn -q "
    DROP DATABASE IF EXISTS database_for_dict;
    DROP TABLE IF EXISTS table_for_dict1;
    DROP TABLE IF EXISTS table_for_dict2;

    CREATE TABLE table_for_dict1 (key_column UInt64, value_column String) ENGINE = MergeTree ORDER BY key_column;
    CREATE TABLE table_for_dict2 (key_column UInt64, value_column String) ENGINE = MergeTree ORDER BY key_column;

    INSERT INTO table_for_dict1 SELECT number, toString(number) from numbers(1000);
    INSERT INTO table_for_dict2 SELECT number, toString(number) from numbers(1000, 1000);

    CREATE DATABASE database_for_dict;

    CREATE DICTIONARY database_for_dict.dict1 (key_column UInt64, value_column String)
    PRIMARY KEY key_column
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict1' PASSWORD '' DB '$CLICKHOUSE_DATABASE'))
    LIFETIME(MIN 1 MAX 5)
    LAYOUT(FLAT());

    CREATE DICTIONARY database_for_dict.dict2 (key_column UInt64, value_column String)
    PRIMARY KEY key_column
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict2' PASSWORD '' DB '$CLICKHOUSE_DATABASE'))
    LIFETIME(MIN 1 MAX 5)
    LAYOUT(CACHE(SIZE_IN_CELLS 150));
"


function thread1()
{
    $CLICKHOUSE_CLIENT --query "SELECT * FROM system.dictionaries FORMAT Null"
}

function thread2()
{
    $CLICKHOUSE_CLIENT --query "ATTACH DICTIONARY database_for_dict.dict1" ||:
}

function thread3()
{
    $CLICKHOUSE_CLIENT --query "ATTACH DICTIONARY database_for_dict.dict2" ||:
}


function thread4()
{
    $CLICKHOUSE_CLIENT -n -q "
        SELECT * FROM database_for_dict.dict1 FORMAT Null;
        SELECT * FROM database_for_dict.dict2 FORMAT Null;
    " ||:
}

function thread5()
{
    $CLICKHOUSE_CLIENT -n -q "
        SELECT dictGetString('database_for_dict.dict1', 'value_column', toUInt64(number)) from numbers(1000) FROM FORMAT Null;
        SELECT dictGetString('database_for_dict.dict2', 'value_column', toUInt64(number)) from numbers(1000) FROM FORMAT Null;
    " ||:
}

function thread6()
{
    $CLICKHOUSE_CLIENT -q "DETACH DICTIONARY database_for_dict.dict1"
}

function thread7()
{
    $CLICKHOUSE_CLIENT -q "DETACH DICTIONARY database_for_dict.dict2"
}


export -f thread1
export -f thread2
export -f thread3
export -f thread4
export -f thread5
export -f thread6
export -f thread7

TIMEOUT=10

clickhouse_client_loop_timeout $TIMEOUT thread1 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread2 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread3 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread4 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread5 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread6 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread7 2> /dev/null &

clickhouse_client_loop_timeout $TIMEOUT thread1 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread2 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread3 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread4 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread5 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread6 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread7 2> /dev/null &

clickhouse_client_loop_timeout $TIMEOUT thread1 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread2 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread3 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread4 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread5 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread6 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread7 2> /dev/null &

clickhouse_client_loop_timeout $TIMEOUT thread1 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread2 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread3 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread4 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread5 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread6 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread7 2> /dev/null &

wait
$CLICKHOUSE_CLIENT -q "SELECT 'Still alive'"

$CLICKHOUSE_CLIENT -q "ATTACH DICTIONARY IF NOT EXISTS database_for_dict.dict1"
$CLICKHOUSE_CLIENT -q "ATTACH DICTIONARY IF NOT EXISTS database_for_dict.dict2"

$CLICKHOUSE_CLIENT -n -q "
    DROP DATABASE database_for_dict;
    DROP TABLE table_for_dict1;
    DROP TABLE table_for_dict2;
"
