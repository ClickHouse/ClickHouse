#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -q "
    DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE_1};
    DROP TABLE IF EXISTS table_for_dict1;
    DROP TABLE IF EXISTS table_for_dict2;

    CREATE TABLE table_for_dict1 (key_column UInt64, value_column String) ENGINE = MergeTree ORDER BY key_column;
    CREATE TABLE table_for_dict2 (key_column UInt64, value_column String) ENGINE = MergeTree ORDER BY key_column;

    INSERT INTO table_for_dict1 SELECT number, toString(number) from numbers(1000);
    INSERT INTO table_for_dict2 SELECT number, toString(number) from numbers(1000, 1000);

    CREATE DATABASE ${CLICKHOUSE_DATABASE_1};

    CREATE DICTIONARY ${CLICKHOUSE_DATABASE_1}.dict1 (key_column UInt64, value_column String) PRIMARY KEY key_column SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict1' PASSWORD '' DB '$CLICKHOUSE_DATABASE')) LIFETIME(MIN 1 MAX 5) LAYOUT(FLAT());

    CREATE DICTIONARY ${CLICKHOUSE_DATABASE_1}.dict2 (key_column UInt64, value_column String) PRIMARY KEY key_column SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict2' PASSWORD '' DB '$CLICKHOUSE_DATABASE')) LIFETIME(MIN 1 MAX 5) LAYOUT(CACHE(SIZE_IN_CELLS 150));
"


function thread1()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query "SELECT * FROM system.dictionaries FORMAT Null"
    done
}

function thread2()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        CLICKHOUSE_CLIENT --query "ATTACH DICTIONARY ${CLICKHOUSE_DATABASE_1}.dict1" ||:
    done
}

function thread3()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        CLICKHOUSE_CLIENT --query "ATTACH DICTIONARY ${CLICKHOUSE_DATABASE_1}.dict2" ||:
    done
}


function thread4()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT -q "
            SELECT * FROM ${CLICKHOUSE_DATABASE_1}.dict1 FORMAT Null;
            SELECT * FROM ${CLICKHOUSE_DATABASE_1}.dict2 FORMAT Null;
        " ||:
    done
}

function thread5()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT -q "
            SELECT dictGetString('${CLICKHOUSE_DATABASE_1}.dict1', 'value_column', toUInt64(number)) from numbers(1000) FROM FORMAT Null;
            SELECT dictGetString('${CLICKHOUSE_DATABASE_1}.dict2', 'value_column', toUInt64(number)) from numbers(1000) FROM FORMAT Null;
        " ||:
    done
}

function thread6()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT -q "DETACH DICTIONARY ${CLICKHOUSE_DATABASE_1}.dict1"
    done
}

function thread7()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT -q "DETACH DICTIONARY ${CLICKHOUSE_DATABASE_1}.dict2"
    done
}

TIMEOUT=10

thread1 2> /dev/null &
thread2 2> /dev/null &
thread3 2> /dev/null &
thread4 2> /dev/null &
thread5 2> /dev/null &
thread6 2> /dev/null &
thread7 2> /dev/null &

thread1 2> /dev/null &
thread2 2> /dev/null &
thread3 2> /dev/null &
thread4 2> /dev/null &
thread5 2> /dev/null &
thread6 2> /dev/null &
thread7 2> /dev/null &

thread1 2> /dev/null &
thread2 2> /dev/null &
thread3 2> /dev/null &
thread4 2> /dev/null &
thread5 2> /dev/null &
thread6 2> /dev/null &
thread7 2> /dev/null &

thread1 2> /dev/null &
thread2 2> /dev/null &
thread3 2> /dev/null &
thread4 2> /dev/null &
thread5 2> /dev/null &
thread6 2> /dev/null &
thread7 2> /dev/null &

wait
$CLICKHOUSE_CLIENT -q "SELECT 'Still alive'"

$CLICKHOUSE_CLIENT -q "ATTACH DICTIONARY IF NOT EXISTS ${CLICKHOUSE_DATABASE_1}.dict1"
$CLICKHOUSE_CLIENT -q "ATTACH DICTIONARY IF NOT EXISTS ${CLICKHOUSE_DATABASE_1}.dict2"

$CLICKHOUSE_CLIENT -q "
    DROP DATABASE ${CLICKHOUSE_DATABASE_1};
    DROP TABLE table_for_dict1;
    DROP TABLE table_for_dict2;
"
