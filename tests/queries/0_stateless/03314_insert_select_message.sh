#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Track degradations for issue https://github.com/ClickHouse/ClickHouse/issues/47800

$CLICKHOUSE_CLIENT --query "CREATE DATABASE IF NOT EXISTS test_03314"
$CLICKHOUSE_CLIENT --query "CREATE TABLE test_03314.simple_table (id UInt64, name String) ENGINE = Memory"

$CLICKHOUSE_CLIENT --processed-rows --query "INSERT INTO test_03314.simple_table SELECT 1 AS id, 'Alice' AS name"
$CLICKHOUSE_CLIENT --processed-rows --query "INSERT INTO test_03314.simple_table SELECT number + 1 AS id, ['Alice','Bob','Charlie'][number + 1] AS name FROM numbers(3)"

$CLICKHOUSE_CLIENT --query "DROP TABLE test_03314.simple_table"
$CLICKHOUSE_CLIENT --query "DROP DATABASE test_03314"
