#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS 02154_test_source_table"

$CLICKHOUSE_CLIENT -q """
    CREATE TABLE 02154_test_source_table
    (
        id UInt64,
        value String
    ) ENGINE=TinyLog;
"""

$CLICKHOUSE_CLIENT -q "INSERT INTO 02154_test_source_table VALUES (0, 'Value')"
$CLICKHOUSE_CLIENT -q "SELECT * FROM 02154_test_source_table"

$CLICKHOUSE_CLIENT -q "DROP DICTIONARY IF EXISTS 02154_test_dictionary"
$CLICKHOUSE_CLIENT -q """
    CREATE DICTIONARY 02154_test_dictionary
    (
        id UInt64,
        value String
    )
    PRIMARY KEY id
    LAYOUT(HASHED())
    LIFETIME(0)
    SOURCE(CLICKHOUSE(TABLE '02154_test_source_table'))
"""

echo """
    SELECT dictGet(02154_test_dictionary, 'value', toUInt64(0)), dictGet(02154_test_dictionary, 'value', toUInt64(1))
    SETTINGS enable_analyzer = 1
    FORMAT JSON
""" | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}&wait_end_of_query=1&output_format_write_statistics=0" -d @-

$CLICKHOUSE_CLIENT -q "DROP DICTIONARY 02154_test_dictionary"
$CLICKHOUSE_CLIENT -q "DROP TABLE 02154_test_source_table"
