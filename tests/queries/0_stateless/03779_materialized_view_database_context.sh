#!/usr/bin/env bash
# Test that materialized views use the correct database context when calling
# parameterized views (issue #87082)

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB1="${CLICKHOUSE_DATABASE}_1"
DB2="${CLICKHOUSE_DATABASE}_2"

# Clean up
$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS $DB1"
$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS $DB2"

# Setup db1 with table, parameterized view, and REFRESH MV
$CLICKHOUSE_CLIENT --query "CREATE DATABASE $DB1"
$CLICKHOUSE_CLIENT --query "CREATE TABLE $DB1.tbl1 (id Int32, data String) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT --query "INSERT INTO $DB1.tbl1 VALUES (1, 'db1_data')"
$CLICKHOUSE_CLIENT --query "CREATE VIEW $DB1.v1 AS SELECT id, data, {p:String} as p FROM $DB1.tbl1"
$CLICKHOUSE_CLIENT --query "CREATE MATERIALIZED VIEW $DB1.mv1 REFRESH EVERY 1 SECOND ORDER BY id AS SELECT id, data, p FROM $DB1.v1(p='from_mv1_db1')"
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT VIEW $DB1.mv1"
$CLICKHOUSE_CLIENT --query "SELECT 'mv1_db1:', * FROM $DB1.mv1 ORDER BY id"

# Setup second database with same structure but different data
$CLICKHOUSE_CLIENT --query "CREATE DATABASE $DB2"
$CLICKHOUSE_CLIENT --query "CREATE TABLE $DB2.tbl1 (id Int32, data String) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT --query "INSERT INTO $DB2.tbl1 VALUES (2, 'db2_data')"
$CLICKHOUSE_CLIENT --query "CREATE VIEW $DB2.v1 AS SELECT id, data, {p:String} as p FROM $DB2.tbl1"
$CLICKHOUSE_CLIENT --query "CREATE MATERIALIZED VIEW $DB2.mv1 REFRESH EVERY 1 SECOND ORDER BY id AS SELECT id, data, p FROM $DB2.v1(p='from_mv1_db2')"
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT VIEW $DB2.mv1"
$CLICKHOUSE_CLIENT --query "SELECT 'mv1_db2:', * FROM $DB2.mv1 ORDER BY id"

# Test that qualified references still work
$CLICKHOUSE_CLIENT --query "CREATE MATERIALIZED VIEW $DB1.mv_qualified_crossdb REFRESH EVERY 1 SECOND ORDER BY id AS SELECT id, data, p FROM $DB2.v1(p='qualified_crossdb')"
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT VIEW $DB1.mv_qualified_crossdb"
$CLICKHOUSE_CLIENT --query "SELECT 'qualified:', * FROM $DB1.mv_qualified_crossdb ORDER BY id"

# Create second set of tables and views for testing ALTER MODIFY QUERY
$CLICKHOUSE_CLIENT --query "CREATE TABLE $DB1.tbl2 (id Int32, data String) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT --query "INSERT INTO $DB1.tbl2 VALUES (3, 'db1_v2_data')"
$CLICKHOUSE_CLIENT --query "CREATE VIEW $DB1.v2 AS SELECT id, data, {p:String} as p FROM $DB1.tbl2"

$CLICKHOUSE_CLIENT --query "CREATE TABLE $DB2.tbl2 (id Int32, data String) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT --query "INSERT INTO $DB2.tbl2 VALUES (4, 'db2_v2_data')"
$CLICKHOUSE_CLIENT --query "CREATE VIEW $DB2.v2 AS SELECT id, data, {p:String} as p FROM $DB2.tbl2"

# Test ALTER TABLE ... MODIFY QUERY uses correct database context
$CLICKHOUSE_CLIENT --query "ALTER TABLE $DB1.mv1 MODIFY QUERY SELECT id, data, p FROM v2(p='modified_query_db1')"
$CLICKHOUSE_CLIENT --query "SYSTEM REFRESH VIEW $DB1.mv1"
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT VIEW $DB1.mv1"
$CLICKHOUSE_CLIENT --query "SELECT 'modified_mv1_db1:', * FROM $DB1.mv1 ORDER BY id"

$CLICKHOUSE_CLIENT --query "ALTER TABLE $DB2.mv1 MODIFY QUERY SELECT id, data, p FROM v2(p='modified_query_db2')"
$CLICKHOUSE_CLIENT --query "SYSTEM REFRESH VIEW $DB2.mv1"
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT VIEW $DB2.mv1"
$CLICKHOUSE_CLIENT --query "SELECT 'modified_mv1_db2:', * FROM $DB2.mv1 ORDER BY id"

# Cleanup
$CLICKHOUSE_CLIENT --query "DROP DATABASE $DB1"
$CLICKHOUSE_CLIENT --query "DROP DATABASE $DB2"
