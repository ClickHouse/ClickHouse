#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Track degradations for issue https://github.com/ClickHouse/ClickHouse/issues/47800

$CLICKHOUSE_CLIENT --query "CREATE TABLE IF NOT EXISTS test (id UInt64, name String) ENGINE = Memory"

# Test 1: INSERT-SELECT (processed_rows_from_progress)
$CLICKHOUSE_CLIENT --processed-rows --query "INSERT INTO test SELECT number AS id, toString(number) AS name FROM numbers(100)"

# Test 2: Large INSERT-SELECT (processed_rows_from_progress)
$CLICKHOUSE_CLIENT --processed-rows --query "INSERT INTO test SELECT number AS id, toString(number) AS name FROM numbers(1000000)"

# Test 3: Regular SELECT (processed_rows_from_blocks)
$CLICKHOUSE_CLIENT --processed-rows --query "SELECT * FROM test ORDER BY id LIMIT 30"

# Test 4: INSERT with Materialized View (to verify we correctly show that data was inserted twice)
$CLICKHOUSE_CLIENT --query "CREATE MATERIALIZED VIEW IF NOT EXISTS mv ENGINE = Memory AS SELECT id FROM test"
$CLICKHOUSE_CLIENT --processed-rows --query "INSERT INTO test SELECT number AS id, toString(number) AS name FROM numbers(50)"

# Cleanup
$CLICKHOUSE_CLIENT --query "DROP VIEW IF EXISTS mv"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test"
