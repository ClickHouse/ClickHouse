#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS db_01038"

$CLICKHOUSE_CLIENT --query "CREATE DATABASE db_01038"


$CLICKHOUSE_CLIENT --query "
CREATE TABLE db_01038.table_for_dict
(
  key_column UInt64,
  value Float64
)
ENGINE = MergeTree()
ORDER BY key_column"

$CLICKHOUSE_CLIENT --query "INSERT INTO db_01038.table_for_dict VALUES (1, 1.1)"

$CLICKHOUSE_CLIENT --query "
CREATE DICTIONARY db_01038.dict_with_zero_min_lifetime
(
    key_column UInt64,
    value Float64 DEFAULT 77.77
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'table_for_dict' DB 'db_01038'))
LIFETIME(1)
LAYOUT(FLAT())"

$CLICKHOUSE_CLIENT --query "SELECT dictGetFloat64('db_01038.dict_with_zero_min_lifetime', 'value', toUInt64(1))"

$CLICKHOUSE_CLIENT --query "SELECT dictGetFloat64('db_01038.dict_with_zero_min_lifetime', 'value', toUInt64(2))"

$CLICKHOUSE_CLIENT --query "INSERT INTO db_01038.table_for_dict VALUES (2, 2.2)"


function check()
{

    query_result=$($CLICKHOUSE_CLIENT --query "SELECT dictGetFloat64('db_01038.dict_with_zero_min_lifetime', 'value', toUInt64(2))")

    while [ "$query_result" != "2.2" ]
    do
        query_result=$($CLICKHOUSE_CLIENT --query "SELECT dictGetFloat64('db_01038.dict_with_zero_min_lifetime', 'value', toUInt64(2))")
    done
}


export -f check;

timeout 10 bash -c check

$CLICKHOUSE_CLIENT --query "SELECT dictGetFloat64('db_01038.dict_with_zero_min_lifetime', 'value', toUInt64(1))"

$CLICKHOUSE_CLIENT --query "SELECT dictGetFloat64('db_01038.dict_with_zero_min_lifetime', 'value', toUInt64(2))"

$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS db_01038"
