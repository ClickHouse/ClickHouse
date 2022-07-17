#!/usr/bin/env bash
# Tags: race, no-parallel, no-random-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ordinary_db"

$CLICKHOUSE_CLIENT --query "CREATE DATABASE ordinary_db"

$CLICKHOUSE_CLIENT -n -q "

CREATE DICTIONARY ordinary_db.dict1
(
  key_column UInt64 DEFAULT 0,
  second_column UInt8 DEFAULT 1,
  third_column String DEFAULT 'qqq'
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'view_for_dict' PASSWORD '' DB 'ordinary_db'))
LIFETIME(MIN 1 MAX 3)
LAYOUT(CACHE(SIZE_IN_CELLS 3));
"

function dict_get_thread()
{
    while true; do
        $CLICKHOUSE_CLIENT --query "SELECT dictGetString('ordinary_db.dict1', 'third_column', toUInt64(rand() % 1000)) from numbers(2)" &>/dev/null
    done
}


function drop_create_table_thread()
{
    while true; do
        $CLICKHOUSE_CLIENT -n --query "CREATE TABLE ordinary_db.table_for_dict_real (
            key_column UInt64,
            second_column UInt8,
            third_column String
        )
        ENGINE MergeTree() ORDER BY tuple();
        INSERT INTO ordinary_db.table_for_dict_real SELECT number, number, toString(number) from numbers(2);
        CREATE VIEW ordinary_db.view_for_dict AS SELECT key_column, second_column, third_column from ordinary_db.table_for_dict_real WHERE sleepEachRow(1) == 0 SETTINGS max_block_size=1;
"
        sleep 10

        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ordinary_db.table_for_dict_real"
        sleep 10
    done
}

export -f dict_get_thread;
export -f drop_create_table_thread;

TIMEOUT=30

timeout $TIMEOUT bash -c dict_get_thread 2> /dev/null &
timeout $TIMEOUT bash -c dict_get_thread 2> /dev/null &
timeout $TIMEOUT bash -c dict_get_thread 2> /dev/null &
timeout $TIMEOUT bash -c dict_get_thread 2> /dev/null &
timeout $TIMEOUT bash -c dict_get_thread 2> /dev/null &
timeout $TIMEOUT bash -c dict_get_thread 2> /dev/null &


timeout $TIMEOUT bash -c drop_create_table_thread 2> /dev/null &

wait



$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ordinary_db"
