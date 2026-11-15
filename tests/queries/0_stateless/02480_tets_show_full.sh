#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


database=$($CLICKHOUSE_CLIENT -q 'SELECT currentDatabase()')

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS test_02480_table;
DROP VIEW IF EXISTS test_02480_view;
CREATE TABLE test_02480_table (id Int64) ENGINE=MergeTree ORDER BY id;
CREATE VIEW test_02480_view AS SELECT * FROM test_02480_table;
SHOW FULL TABLES FROM $database LIKE '%';
DROP TABLE IF EXISTS test_02480_table;
DROP VIEW IF EXISTS test_02480_view;
"
