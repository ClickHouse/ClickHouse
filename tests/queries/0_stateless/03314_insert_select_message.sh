#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Track degradations for issue https://github.com/ClickHouse/ClickHouse/issues/47800

$CLICKHOUSE_CLIENT --query "CREATE DATABASE IF NOT EXISTS test_03314"
$CLICKHOUSE_CLIENT --query "CREATE TABLE IF NOT EXISTS test_03314.table (id UInt64, name String) ENGINE = Memory"

# Test 1: INSERT-SELECT (processed_rows_from_progress)
$CLICKHOUSE_CLIENT --processed-rows --query "INSERT INTO test_03314.table SELECT number AS id, toString(number) AS name FROM numbers(100)"

# Test 2: Large INSERT-SELECT (processed_rows_from_progress)
$CLICKHOUSE_CLIENT --processed-rows --query "INSERT INTO test_03314.table SELECT number AS id, toString(number) AS name FROM numbers(1000000)"

# Test 3: Regular SELECT (processed_rows_from_blocks)
$CLICKHOUSE_CLIENT --processed-rows --query "SELECT * FROM test_03314.table ORDER BY id LIMIT 30"

# Test 4: INSERT with Materialized View (to verify we correctly show that data was inserted twice)
$CLICKHOUSE_CLIENT --query "CREATE MATERIALIZED VIEW IF NOT EXISTS test_03314.mv ENGINE = Memory AS SELECT id FROM test_03314.table"
$CLICKHOUSE_CLIENT --processed-rows --query "INSERT INTO test_03314.table SELECT number AS id, toString(number) AS name FROM numbers(50)"

# Cleanup
$CLICKHOUSE_CLIENT --query "DROP VIEW IF EXISTS test_03314.mv"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_03314.table"
$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS test_03314"
