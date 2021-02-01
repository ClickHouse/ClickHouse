#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS empty_strings_deserialization"
$CLICKHOUSE_CLIENT -q "CREATE TABLE empty_strings_deserialization(s String, i Int32, f Float32) ENGINE Memory"

echo ',,' | $CLICKHOUSE_CLIENT -q "INSERT INTO empty_strings_deserialization FORMAT CSV"
echo 'aaa,-,' | $CLICKHOUSE_CLIENT -q "INSERT INTO empty_strings_deserialization FORMAT CSV"
echo 'bbb,,-' | $CLICKHOUSE_CLIENT -q "INSERT INTO empty_strings_deserialization FORMAT CSV"

$CLICKHOUSE_CLIENT -q "SELECT * FROM empty_strings_deserialization ORDER BY s"

$CLICKHOUSE_CLIENT -q "DROP TABLE empty_strings_deserialization"
