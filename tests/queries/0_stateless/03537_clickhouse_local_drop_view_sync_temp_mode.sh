#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Cleanup any existing objects
${CLICKHOUSE_LOCAL} -q "
DROP VIEW IF EXISTS v0;
DROP VIEW IF EXISTS v1;
DROP VIEW IF EXISTS test_view;
DROP VIEW IF EXISTS temp_view;
DROP TEMPORARY TABLE IF EXISTS temp_table;
"

# Create view and drop with SYNC - should not hang with clickhouse-local
${CLICKHOUSE_LOCAL} -q "
CREATE VIEW v1 AS (SELECT 1 AS c1);
CREATE VIEW test_view AS (SELECT number, number * 2 AS doubled FROM numbers(5));
DROP VIEW v1 SYNC;
DROP VIEW test_view SYNC;
"

# Test with temporary table and view should work without `--path` in clickhouse-local
${CLICKHOUSE_LOCAL} -q "
CREATE TEMPORARY TABLE temp_table (x UInt64, y String) ENGINE = Memory;
INSERT INTO temp_table VALUES (1, 'a'), (2, 'b');
CREATE VIEW temp_view AS SELECT * FROM temp_table;
DROP VIEW temp_view SYNC;
DROP TEMPORARY TABLE IF EXISTS temp_table;
"
