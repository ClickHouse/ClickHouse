#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS test_db";
${CLICKHOUSE_CLIENT} --param_database="test_db" --query "CREATE DATABASE {database:Identifier}";
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_db.test_parameters (id UInt64, col1 UInt64) ENGINE = MergeTree() ORDER BY id";
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE test_db.test_parameters";
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_db.test_parameters VALUES (0,0)";
${CLICKHOUSE_CLIENT} --param_table="test_parameters" --query "OPTIMIZE TABLE test_db.{table:Identifier}";
${CLICKHOUSE_CLIENT} --param_table="test_parameters" --query "ALTER TABLE test_db.{table:Identifier} RENAME COLUMN col1 to col2";
${CLICKHOUSE_CLIENT} --param_table="test_parameters" --query "EXISTS TABLE test_db.{table:Identifier}";
${CLICKHOUSE_CLIENT} --param_table="test_parameters" --query "DROP TABLE test_db.{table:Identifier}";
${CLICKHOUSE_CLIENT} --param_database="test_db" --query "DROP DATABASE {database:Identifier}";
