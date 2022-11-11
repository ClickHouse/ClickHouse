#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_source_table_02154"

$CLICKHOUSE_CLIENT -q """
    CREATE TABLE test_source_table_02154
    (
        id UInt64,
        value String
    ) ENGINE=TinyLog;
"""

$CLICKHOUSE_CLIENT -q "INSERT INTO test_source_table_02154 VALUES (0, 'Value')"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_source_table_02154"

$CLICKHOUSE_CLIENT -q "DROP DICTIONARY IF EXISTS test_dictionary_02154"
$CLICKHOUSE_CLIENT -q """
    CREATE DICTIONARY test_dictionary_02154
    (
        id UInt64,
        value String
    )
    PRIMARY KEY id
    LAYOUT(HASHED())
    LIFETIME(0)
    SOURCE(CLICKHOUSE(TABLE 'test_source_table_02154'))
"""

echo """
    SELECT dictGet(test_dictionary_02154, 'value', toUInt64(0)), dictGet(test_dictionary_02154, 'value', toUInt64(1))
    FORMAT JSON
""" | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}&wait_end_of_query=1&output_format_write_statistics=0" -d @-

$CLICKHOUSE_CLIENT -q "DROP DICTIONARY test_dictionary_02154"
$CLICKHOUSE_CLIENT -q "DROP TABLE test_source_table_02154"
