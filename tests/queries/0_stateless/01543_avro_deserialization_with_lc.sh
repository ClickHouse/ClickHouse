#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "CREATE TABLE IF NOT EXISTS test_01543 (value LowCardinality(String)) ENGINE=Memory()"
$CLICKHOUSE_CLIENT --query "INSERT INTO test_01543 SELECT toString(number) FROM numbers(1000)"

$CLICKHOUSE_CLIENT -q "SELECT * FROM test_01543 FORMAT Avro" |
    $CLICKHOUSE_CLIENT -q "INSERT INTO test_01543 FORMAT Avro";

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_01543"
