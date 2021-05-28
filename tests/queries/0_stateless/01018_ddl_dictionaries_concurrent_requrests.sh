#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -n -q "
    DROP DATABASE IF EXISTS database_for_dict;
    DROP TABLE IF EXISTS table_for_dict1;
    DROP TABLE IF EXISTS table_for_dict2;

    CREATE TABLE table_for_dict1 (key_column UInt64, value_column String) ENGINE = MergeTree ORDER BY key_column;
    CREATE TABLE table_for_dict2 (key_column UInt64, value_column String) ENGINE = MergeTree ORDER BY key_column;

    INSERT INTO table_for_dict1 SELECT number, toString(number) from numbers(1000);
    INSERT INTO table_for_dict2 SELECT number, toString(number) from numbers(1000, 1000);

    CREATE DATABASE database_for_dict;

    CREATE DICTIONARY database_for_dict.dict1 (key_column UInt64, value_column String) PRIMARY KEY key_column SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'table_for_dict1' PASSWORD '' DB '$CLICKHOUSE_DATABASE')) LIFETIME(MIN 1 MAX 5) LAYOUT(FLAT());

    CREATE DICTIONARY database_for_dict.dict2 (key_column UInt64, value_column String) PRIMARY KEY key_column SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'table_for_dict2' PASSWORD '' DB '$CLICKHOUSE_DATABASE')) LIFETIME(MIN 1 MAX 5) LAYOUT(CACHE(SIZE_IN_CELLS 150));
"


function thread1()
{
    while true; do $CLICKHOUSE_CLIENT --query "SELECT * FROM system.dictionaries FORMAT Null"; done
}

function thread2()
{
    while true; do CLICKHOUSE_CLIENT --query "ATTACH DICTIONARY database_for_dict.dict1" ||: ; done
}

function thread3()
{
    while true; do CLICKHOUSE_CLIENT --query "ATTACH DICTIONARY database_for_dict.dict2" ||:; done
}


function thread4()
{
    while true; do $CLICKHOUSE_CLIENT -n -q "
        SELECT * FROM database_for_dict.dict1 FORMAT Null;
        SELECT * FROM database_for_dict.dict2 FORMAT Null;
    " ||: ; done
}

function thread5()
{
    while true; do $CLICKHOUSE_CLIENT -n -q "
        SELECT dictGetString('database_for_dict.dict1', 'value_column', toUInt64(number)) from numbers(1000) FROM FORMAT Null;
        SELECT dictGetString('database_for_dict.dict2', 'value_column', toUInt64(number)) from numbers(1000) FROM FORMAT Null;
    " ||: ; done
}

function thread6()
{
    while true; do $CLICKHOUSE_CLIENT -q "DETACH DICTIONARY database_for_dict.dict1"; done
}

function thread7()
{
    while true; do $CLICKHOUSE_CLIENT -q "DETACH DICTIONARY database_for_dict.dict2"; done
}


export -f thread1;
export -f thread2;
export -f thread3;
export -f thread4;
export -f thread5;
export -f thread6;
export -f thread7;

TIMEOUT=10

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &
timeout $TIMEOUT bash -c thread3 2> /dev/null &
timeout $TIMEOUT bash -c thread4 2> /dev/null &
timeout $TIMEOUT bash -c thread5 2> /dev/null &
timeout $TIMEOUT bash -c thread6 2> /dev/null &
timeout $TIMEOUT bash -c thread7 2> /dev/null &

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &
timeout $TIMEOUT bash -c thread3 2> /dev/null &
timeout $TIMEOUT bash -c thread4 2> /dev/null &
timeout $TIMEOUT bash -c thread5 2> /dev/null &
timeout $TIMEOUT bash -c thread6 2> /dev/null &
timeout $TIMEOUT bash -c thread7 2> /dev/null &

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &
timeout $TIMEOUT bash -c thread3 2> /dev/null &
timeout $TIMEOUT bash -c thread4 2> /dev/null &
timeout $TIMEOUT bash -c thread5 2> /dev/null &
timeout $TIMEOUT bash -c thread6 2> /dev/null &
timeout $TIMEOUT bash -c thread7 2> /dev/null &

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &
timeout $TIMEOUT bash -c thread3 2> /dev/null &
timeout $TIMEOUT bash -c thread4 2> /dev/null &
timeout $TIMEOUT bash -c thread5 2> /dev/null &
timeout $TIMEOUT bash -c thread6 2> /dev/null &
timeout $TIMEOUT bash -c thread7 2> /dev/null &

wait
$CLICKHOUSE_CLIENT -q "SELECT 'Still alive'"

$CLICKHOUSE_CLIENT -q "ATTACH DICTIONARY database_for_dict.dict1"
$CLICKHOUSE_CLIENT -q "ATTACH DICTIONARY database_for_dict.dict2"

$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE table_for_dict1;
    DROP TABLE table_for_dict2;
    DROP DATABASE database_for_dict;
"
