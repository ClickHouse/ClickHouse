#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS database_for_dict"

$CLICKHOUSE_CLIENT --query "CREATE DATABASE database_for_dict Engine = Ordinary"


$CLICKHOUSE_CLIENT --query "
CREATE TABLE database_for_dict.table_for_dict
(
  key_column UInt64,
  value Float64
)
ENGINE = MergeTree()
ORDER BY key_column"

$CLICKHOUSE_CLIENT --query "INSERT INTO database_for_dict.table_for_dict VALUES (1, 1.1)"

$CLICKHOUSE_CLIENT --query "
CREATE DICTIONARY database_for_dict.dict_with_zero_min_lifetime
(
    key_column UInt64,
    value Float64 DEFAULT 77.77
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'table_for_dict' DB 'database_for_dict'))
LIFETIME(1)
LAYOUT(FLAT())"

$CLICKHOUSE_CLIENT --query "SELECT dictGetFloat64('database_for_dict.dict_with_zero_min_lifetime', 'value', toUInt64(1))"

$CLICKHOUSE_CLIENT --query "SELECT dictGetFloat64('database_for_dict.dict_with_zero_min_lifetime', 'value', toUInt64(2))"

$CLICKHOUSE_CLIENT --query "INSERT INTO database_for_dict.table_for_dict VALUES (2, 2.2)"


function check()
{

    query_result=$($CLICKHOUSE_CLIENT --query "SELECT dictGetFloat64('database_for_dict.dict_with_zero_min_lifetime', 'value', toUInt64(2))")

    while [ "$query_result" != "2.2" ]
    do
        query_result=$($CLICKHOUSE_CLIENT --query "SELECT dictGetFloat64('database_for_dict.dict_with_zero_min_lifetime', 'value', toUInt64(2))")
    done
}


export -f check;

timeout 10 bash -c check

$CLICKHOUSE_CLIENT --query "SELECT dictGetFloat64('database_for_dict.dict_with_zero_min_lifetime', 'value', toUInt64(1))"

$CLICKHOUSE_CLIENT --query "SELECT dictGetFloat64('database_for_dict.dict_with_zero_min_lifetime', 'value', toUInt64(2))"

$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS database_for_dict"
