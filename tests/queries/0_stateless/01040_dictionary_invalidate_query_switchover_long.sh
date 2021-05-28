#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS dictdb"

$CLICKHOUSE_CLIENT --query "CREATE DATABASE dictdb Engine = Ordinary"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE dictdb.dict_invalidate
ENGINE = Memory AS
SELECT
    122 as dummy,
    toDateTime('2019-10-29 18:51:35') AS last_time
FROM system.one"


$CLICKHOUSE_CLIENT --query "
CREATE DICTIONARY dictdb.invalidate
(
  dummy UInt64,
  two UInt8 EXPRESSION dummy
)
PRIMARY KEY dummy
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'dict_invalidate' DB 'dictdb' INVALIDATE_QUERY 'select max(last_time) from dictdb.dict_invalidate'))
LIFETIME(MIN 0 MAX 1)
LAYOUT(FLAT())"

$CLICKHOUSE_CLIENT --query "SELECT dictGetUInt8('dictdb.invalidate', 'two', toUInt64(122))"

$CLICKHOUSE_CLIENT --query "SELECT last_exception FROM system.dictionaries WHERE database = 'dictdb' AND name = 'invalidate'"

# Bad solution, but it's quite complicated to detect, that invalidte_query stopped updates.
# In worst case we don't check anything, but fortunately it doesn't lead to false negatives.
sleep 5

$CLICKHOUSE_CLIENT --query "DROP TABLE dictdb.dict_invalidate"

function check_exception_detected()
{

    query_result=$($CLICKHOUSE_CLIENT --query "SELECT last_exception FROM system.dictionaries WHERE database = 'dictdb' AND name = 'invalidate'" 2>&1)

    while [ -z "$query_result" ]
    do
        query_result=$($CLICKHOUSE_CLIENT --query "SELECT last_exception FROM system.dictionaries WHERE database = 'dictdb' AND name = 'invalidate'" 2>&1)
        sleep 0.1
    done
}


export -f check_exception_detected;
timeout 10 bash -c check_exception_detected 2> /dev/null

$CLICKHOUSE_CLIENT --query "SELECT last_exception FROM system.dictionaries WHERE database = 'dictdb' AND name = 'invalidate'" 2>&1 | grep -Eo "Table dictdb.dict_invalidate .* exist."

$CLICKHOUSE_CLIENT --query "
CREATE TABLE dictdb.dict_invalidate
ENGINE = Memory AS
SELECT
    133 as dummy,
    toDateTime('2019-10-29 18:51:35') AS last_time
FROM system.one"

function check_exception_fixed()
{
    query_result=$($CLICKHOUSE_CLIENT --query "SELECT last_exception FROM system.dictionaries WHERE database = 'dictdb' AND name = 'invalidate'" 2>&1)

    while [ "$query_result" ]
    do
        query_result=$($CLICKHOUSE_CLIENT --query "SELECT last_exception FROM system.dictionaries WHERE database = 'dictdb' AND name = 'invalidate'" 2>&1)
        sleep 0.1
    done
}

export -f check_exception_fixed;
timeout 10 bash -c check_exception_fixed 2> /dev/null

$CLICKHOUSE_CLIENT --query "SELECT last_exception FROM system.dictionaries WHERE database = 'dictdb' AND name = 'invalidate'" 2>&1
$CLICKHOUSE_CLIENT --query "SELECT dictGetUInt8('dictdb.invalidate', 'two', toUInt64(133))"

$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS dictdb"
