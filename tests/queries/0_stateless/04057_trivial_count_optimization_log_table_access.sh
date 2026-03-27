#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS test_table;
    CREATE TABLE test_table (id UInt64, value String) ENGINE=MergeTree ORDER BY id;
    INSERT INTO test_table VALUES (0, 'Value_0');
"

query_id="$RANDOM-$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT --query_id $query_id -q "SELECT COUNT(*) FROM test_table;"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log;"

$CLICKHOUSE_CLIENT -q "SELECT query, length(tables), toString(tables) LIKE '%test_table%' AS contains_test_table FROM system.query_log WHERE query_id = '$query_id' AND type = 'QueryFinish';"

$CLICKHOUSE_CLIENT -q "DROP TABLE test_table;"
