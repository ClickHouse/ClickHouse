#!/usr/bin/env bash
# Tags: race, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE_1}"

$CLICKHOUSE_CLIENT --query "CREATE DATABASE ${CLICKHOUSE_DATABASE_1}"

$CLICKHOUSE_CLIENT -q "

CREATE DICTIONARY ${CLICKHOUSE_DATABASE_1}.dict1
(
  key_column UInt64 DEFAULT 0,
  second_column UInt8 DEFAULT 1,
  third_column String DEFAULT 'qqq'
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'view_for_dict' PASSWORD '' DB '${CLICKHOUSE_DATABASE_1}'))
LIFETIME(MIN 1 MAX 3)
LAYOUT(CACHE(SIZE_IN_CELLS 3));
"

function dict_get_thread()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query "SELECT dictGetString('${CLICKHOUSE_DATABASE_1}.dict1', 'third_column', toUInt64(rand() % 1000)) from numbers(2)" &>/dev/null
    done
}


function drop_create_table_thread()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query "CREATE TABLE ${CLICKHOUSE_DATABASE_1}.table_for_dict_real (
            key_column UInt64,
            second_column UInt8,
            third_column String
        )
        ENGINE MergeTree() ORDER BY tuple();
        INSERT INTO ${CLICKHOUSE_DATABASE_1}.table_for_dict_real SELECT number, number, toString(number) from numbers(2);
        CREATE VIEW ${CLICKHOUSE_DATABASE_1}.view_for_dict AS SELECT key_column, second_column, third_column from ${CLICKHOUSE_DATABASE_1}.table_for_dict_real WHERE sleepEachRow(1) == 0;
"
        sleep 10

        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE_1}.table_for_dict_real"
        sleep 10
    done
}

TIMEOUT=20

dict_get_thread 2> /dev/null &
dict_get_thread 2> /dev/null &
dict_get_thread 2> /dev/null &
dict_get_thread 2> /dev/null &
dict_get_thread 2> /dev/null &
dict_get_thread 2> /dev/null &


drop_create_table_thread 2> /dev/null &

wait



$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE_1}"
