#!/usr/bin/env bash
# Test that materialized views use the correct database context when calling
# parameterized views (issue #87082)

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB1="${CLICKHOUSE_DATABASE}_1"
DB2="${CLICKHOUSE_DATABASE}_2"

# Cleanup any existing test databases
$CLICKHOUSE_CLIENT -q "
    DROP DATABASE IF EXISTS $DB1;
    DROP DATABASE IF EXISTS $DB2
"

# Test original issue #87082: UNQUALIFIED parameterized view references in REFRESH MVs
# When session is in the same database as the MV, CREATE works (session context correct)
# Before fix: RUNTIME execution of the refreshable materialized view failed with "Unknown table function v1"
# After fix: RUNTIME succeeds (context set to MV's database during refresh)

# Setup DB1 with unqualified references (session in DB1)
# Test that it works at runtime (failed before fix) and returns data from DB1
$CLICKHOUSE_CLIENT -m -q "
    CREATE DATABASE $DB1;
    USE $DB1;
    CREATE TABLE tbl1 (id Int32, data String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO tbl1 VALUES (1, 'db1_data');
    CREATE VIEW v1 AS SELECT id, data, {p:String} as p FROM tbl1;
    CREATE MATERIALIZED VIEW mv1 REFRESH EVERY 1 SECOND ORDER BY id AS SELECT id, data, p FROM v1(p='from_mv1_db1');
    SYSTEM WAIT VIEW mv1;
    SELECT 'mv1_db1:', * FROM mv1 ORDER BY id;
"

# Setup second database with same structure (session in DB2)
# Test that it also works at runtime (failed before fix) and returns data from DB2
# This verifies that each MV uses its own database context correctly
$CLICKHOUSE_CLIENT -m -q "
    CREATE DATABASE $DB2;
    USE $DB2;
    CREATE TABLE tbl1 (id Int32, data String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO tbl1 VALUES (2, 'db2_data');
    CREATE VIEW v1 AS SELECT id, data, {p:String} as p FROM tbl1;
    CREATE MATERIALIZED VIEW mv1 REFRESH EVERY 1 SECOND ORDER BY id AS SELECT id, data, p FROM v1(p='from_mv1_db2');
    SYSTEM WAIT VIEW mv1;
    SELECT 'mv1_db2:', * FROM mv1 ORDER BY id;
"

# Test qualified cross-database references
# MV in DB1 reading from DB2.v1 - should work regardless of session database
$CLICKHOUSE_CLIENT -m -q "
    CREATE MATERIALIZED VIEW $DB1.mv_qualified_crossdb REFRESH EVERY 1 SECOND ORDER BY id AS SELECT id, data, p FROM $DB2.v1(p='qualified_crossdb');
    SYSTEM WAIT VIEW $DB1.mv_qualified_crossdb;
    SELECT 'qualified:', * FROM $DB1.mv_qualified_crossdb ORDER BY id;
"

# Setup second set of tables and views for testing ALTER MODIFY QUERY
$CLICKHOUSE_CLIENT -m -q "
    USE $DB1;
    CREATE TABLE tbl2 (id Int32, data String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO tbl2 VALUES (3, 'db1_v2_data');
    CREATE VIEW v2 AS SELECT id, data, {p:String} as p FROM tbl2;

    USE $DB2;
    CREATE TABLE tbl2 (id Int32, data String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO tbl2 VALUES (4, 'db2_v2_data');
    CREATE VIEW v2 AS SELECT id, data, {p:String} as p FROM tbl2;
"

# Test ALTER TABLE ... MODIFY QUERY uses correct database context
# Even with unqualified references, the MV's database context should be used
$CLICKHOUSE_CLIENT -m -q "
    ALTER TABLE $DB1.mv1 MODIFY QUERY SELECT id, data, p FROM v2(p='modified_query_db1');
    SYSTEM REFRESH VIEW $DB1.mv1;
    SYSTEM WAIT VIEW $DB1.mv1;
    SELECT 'modified_mv1_db1:', * FROM $DB1.mv1 ORDER BY id;

    ALTER TABLE $DB2.mv1 MODIFY QUERY SELECT id, data, p FROM v2(p='modified_query_db2');
    SYSTEM REFRESH VIEW $DB2.mv1;
    SYSTEM WAIT VIEW $DB2.mv1;
    SELECT 'modified_mv1_db2:', * FROM $DB2.mv1 ORDER BY id;
"

# NEW scenario from review comments: Session database vs MV database context
# When session is in DB2 but we create/alter MV in DB1 with UNQUALIFIED references,
# those references should resolve in DB1 (MV's database), NOT DB2 (session's database)

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB1; DROP DATABASE IF EXISTS $DB2"

# Setup db1 with table and parameterized view, then db2
$CLICKHOUSE_CLIENT -m -q "
    CREATE DATABASE $DB1;
    CREATE TABLE $DB1.tbl1 (id Int32, data String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO $DB1.tbl1 VALUES (1, 'db1_data');
    CREATE VIEW $DB1.v1 AS SELECT id, data, {p:String} as p FROM $DB1.tbl1;
    CREATE DATABASE $DB2;
"

# KEY TEST: Create MV in DB1 with UNQUALIFIED view reference "v1" (not "DB1.v1")
# while session is in DB2. The unqualified v1 should resolve to DB1.v1 (MV's database),
# not the session's current database (DB2)
$CLICKHOUSE_CLIENT -m -q "
    USE $DB2;
    CREATE MATERIALIZED VIEW $DB1.mv1 REFRESH EVERY 1 SECOND ORDER BY id AS SELECT id, data, p FROM v1(p='from_mv1_db1');
    SYSTEM WAIT VIEW $DB1.mv1;
    SELECT 'mv1_db1:', * FROM $DB1.mv1 ORDER BY id;
"

# Cleanup
$CLICKHOUSE_CLIENT -q "
    DROP DATABASE $DB1;
    DROP DATABASE $DB2
"
