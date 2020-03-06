#!/usr/bin/env bash

# This is a monkey test used to trigger sanitizers. 

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --query="CREATE DATABASE dictdb_01076; "

$CLICKHOUSE_CLIENT --query="
CREATE TABLE dictdb_01076.table_datarace
(
  key_column UInt8,
  value Float64
)
ENGINE = MergeTree()
ORDER BY key_column;
"

$CLICKHOUSE_CLIENT --query="
INSERT INTO dictdb_01076.table_datarace VALUES (1, 1.1), (2, 2.2), (3, 3.3);
"

$CLICKHOUSE_CLIENT --query="
CREATE DICTIONARY IF NOT EXISTS dictdb_01076.dict_datarace
(
  key_column UInt64,
  value Float64 DEFAULT 77.77
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'table_datarace' DB 'dictdb_01076'))
LIFETIME(1)
LAYOUT(CACHE());
"

function thread1()
{
  for attempt_thread1 in {1..50}
  do
  	# This query will be ended with exception, because source dictionary has UInt8 as a key type.
    $CLICKHOUSE_CLIENT --query="SELECT dictGetFloat64('dictdb_01076.dict_datarace', 'value', toUInt64(1));"
  done
}


function thread2()
{
  for attempt_thread2 in {1..50}
  do
  	# This query will be ended with exception, because source dictionary has UInt8 as a key type.
    $CLICKHOUSE_CLIENT --query="SELECT dictGetFloat64('dictdb_01076.dict_datarace', 'value', toUInt64(2));"
  done
}

export -f thread1;
export -f thread2;

TIMEOUT=5

timeout $TIMEOUT bash -c thread1 > /dev/null 2>&1 &
timeout $TIMEOUT bash -c thread2 > /dev/null 2>&1 &

wait

echo OK

$CLICKHOUSE_CLIENT --query="DROP TABLE dictdb_01076.table_datarace;"
$CLICKHOUSE_CLIENT --query="DROP DICTIONARY dictdb_01076.dict_datarace;"
$CLICKHOUSE_CLIENT --query="DROP DATABASE dictdb_01076;"