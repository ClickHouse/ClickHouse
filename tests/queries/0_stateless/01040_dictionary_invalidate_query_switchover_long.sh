#!/usr/bin/env bash
# Tags: long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
CREATE TABLE dict_invalidate
ENGINE = Memory AS
SELECT
    122 as dummy,
    toDateTime('2019-10-29 18:51:35') AS last_time
FROM system.one"


$CLICKHOUSE_CLIENT --query "
CREATE DICTIONARY invalidate
(
  dummy UInt64,
  two UInt8 EXPRESSION dummy
)
PRIMARY KEY dummy
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dict_invalidate' DB currentDatabase() INVALIDATE_QUERY 'select max(last_time) from dict_invalidate'))
LIFETIME(MIN 0 MAX 1)
LAYOUT(FLAT())"

$CLICKHOUSE_CLIENT --query "SELECT dictGetUInt8('invalidate', 'two', toUInt64(122))"

# No exception happened
$CLICKHOUSE_CLIENT --query "SELECT last_exception FROM system.dictionaries WHERE database = currentDatabase() AND name = 'invalidate'"

$CLICKHOUSE_CLIENT --check_table_dependencies=0 --query "DROP TABLE dict_invalidate"

function check_exception_detected()
{

    query_result=$($CLICKHOUSE_CLIENT --query "SELECT last_exception FROM system.dictionaries WHERE database = currentDatabase() AND name = 'invalidate'" 2>&1)

    while [ -z "$query_result" ]
    do
        query_result=$($CLICKHOUSE_CLIENT --query "SELECT last_exception FROM system.dictionaries WHERE database = currentDatabase() AND name = 'invalidate'" 2>&1)
        sleep 0.1
    done
}


export -f check_exception_detected;
timeout 30 bash -c check_exception_detected 2> /dev/null

$CLICKHOUSE_CLIENT --query "SELECT last_exception FROM system.dictionaries WHERE database = currentDatabase() AND name = 'invalidate'" 2>&1 | grep -Eo "dict_invalidate.*UNKNOWN_TABLE" | wc -l

$CLICKHOUSE_CLIENT --query "
CREATE TABLE dict_invalidate
ENGINE = Memory AS
SELECT
    133 as dummy,
    toDateTime('2019-10-29 18:51:35') AS last_time
FROM system.one"

function check_exception_fixed()
{
    query_result=$($CLICKHOUSE_CLIENT --query "SELECT last_exception FROM system.dictionaries WHERE database = currentDatabase() AND name = 'invalidate'" 2>&1)

    while [ "$query_result" ]
    do
        query_result=$($CLICKHOUSE_CLIENT --query "SELECT last_exception FROM system.dictionaries WHERE database = currentDatabase() AND name = 'invalidate'" 2>&1)
        sleep 0.1
    done
}

export -f check_exception_fixed;
# it may take a while until dictionary reloads
timeout 60 bash -c check_exception_fixed 2> /dev/null

$CLICKHOUSE_CLIENT --query "SELECT last_exception FROM system.dictionaries WHERE database = currentDatabase() AND name = 'invalidate'" 2>&1
$CLICKHOUSE_CLIENT --query "SELECT dictGetUInt8('invalidate', 'two', toUInt64(133))"
