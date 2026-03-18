#!/usr/bin/env bash
# Tags: race, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS database_for_dict"
$CLICKHOUSE_CLIENT --query "CREATE DATABASE database_for_dict"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS database_for_dict.table_for_dict"
$CLICKHOUSE_CLIENT --query "CREATE TABLE database_for_dict.table_for_dict (key_column UInt64, second_column UInt64, third_column String) ENGINE = MergeTree() ORDER BY key_column"
$CLICKHOUSE_CLIENT --query "INSERT INTO database_for_dict.table_for_dict VALUES (100500, 10000000, 'Hello world')"

$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ordinary_db"
$CLICKHOUSE_CLIENT --query "CREATE DATABASE ordinary_db"
$CLICKHOUSE_CLIENT --query "CREATE DICTIONARY ordinary_db.dict1 ( key_column UInt64 DEFAULT 0, second_column UInt64 DEFAULT 1, third_column String DEFAULT 'qqq' ) PRIMARY KEY key_column SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' PASSWORD '' DB 'database_for_dict')) LIFETIME(MIN 1 MAX 10) LAYOUT(FLAT()) SETTINGS(max_result_bytes=1)"

function dict_get_thread()
{
    while true; do
        $CLICKHOUSE_CLIENT --query "SELECT dictGetString('ordinary_db.dict1', 'third_column', toUInt64(rand() % 1000)) from numbers(2)" &>/dev/null
    done
}

export -f dict_get_thread;

TIMEOUT=10

timeout $TIMEOUT bash -c dict_get_thread 2> /dev/null &
timeout $TIMEOUT bash -c dict_get_thread 2> /dev/null &
timeout $TIMEOUT bash -c dict_get_thread 2> /dev/null &
timeout $TIMEOUT bash -c dict_get_thread 2> /dev/null &
timeout $TIMEOUT bash -c dict_get_thread 2> /dev/null &
timeout $TIMEOUT bash -c dict_get_thread 2> /dev/null &
timeout $TIMEOUT bash -c dict_get_thread 2> /dev/null &
timeout $TIMEOUT bash -c dict_get_thread 2> /dev/null &
timeout $TIMEOUT bash -c dict_get_thread 2> /dev/null &
timeout $TIMEOUT bash -c dict_get_thread 2> /dev/null &
timeout $TIMEOUT bash -c dict_get_thread 2> /dev/null &
timeout $TIMEOUT bash -c dict_get_thread 2> /dev/null &

wait

$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ordinary_db"
$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS database_for_dict"
