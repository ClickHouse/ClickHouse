#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --query "
CREATE TABLE ${CLICKHOUSE_DATABASE}.table_for_dict
(
  key_column UInt64,
  value Float64
)
ENGINE = MergeTree()
ORDER BY key_column"

$CLICKHOUSE_CLIENT --query "INSERT INTO ${CLICKHOUSE_DATABASE}.table_for_dict VALUES (1, 1.1)"

$CLICKHOUSE_CLIENT --query "
CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.dict_with_zero_min_lifetime
(
    key_column UInt64,
    value Float64 DEFAULT 77.77
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' DB '${CLICKHOUSE_DATABASE}'))
LIFETIME(1)
LAYOUT(FLAT())"

$CLICKHOUSE_CLIENT --query "SELECT dictGetFloat64('${CLICKHOUSE_DATABASE}.dict_with_zero_min_lifetime', 'value', toUInt64(1))"

$CLICKHOUSE_CLIENT --query "SELECT dictGetFloat64('${CLICKHOUSE_DATABASE}.dict_with_zero_min_lifetime', 'value', toUInt64(2))"

$CLICKHOUSE_CLIENT --query "INSERT INTO ${CLICKHOUSE_DATABASE}.table_for_dict VALUES (2, 2.2)"


function check()
{

    query_result=$($CLICKHOUSE_CLIENT --query "SELECT dictGetFloat64('${CLICKHOUSE_DATABASE}.dict_with_zero_min_lifetime', 'value', toUInt64(2))")

    while [ "$query_result" != "2.2" ]
    do
        sleep 0.2
        query_result=$($CLICKHOUSE_CLIENT --query "SELECT dictGetFloat64('${CLICKHOUSE_DATABASE}.dict_with_zero_min_lifetime', 'value', toUInt64(2))")
    done
}


export -f check;

timeout 10 bash -c check

$CLICKHOUSE_CLIENT --query "SELECT dictGetFloat64('${CLICKHOUSE_DATABASE}.dict_with_zero_min_lifetime', 'value', toUInt64(1))"

$CLICKHOUSE_CLIENT --query "SELECT dictGetFloat64('${CLICKHOUSE_DATABASE}.dict_with_zero_min_lifetime', 'value', toUInt64(2))"
