#!/usr/bin/env bash
# Row policies should survive RENAME COLUMN, and dropping a column they use should not be allowed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=$CLICKHOUSE_DATABASE

$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE t (id UInt32, tenant UInt32, note String, extra UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO t VALUES (1, 1, 'a', 0), (2, 2, 'b', 0), (3, 1, 'c', 0);
CREATE ROW POLICY p ON $DB.t FOR SELECT USING tenant = 1 AND length(note) > 0 TO CURRENT_USER;
SELECT 'before', arraySort(groupArray(id)) FROM t;
"

echo "-- dropping a mentioned column is refused"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t DROP COLUMN tenant" 2>&1 | grep -o "ALTER_OF_COLUMN_IS_FORBIDDEN" | head -1
echo "-- dropping an unmentioned column is fine"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t DROP COLUMN extra"

echo "-- the policy follows a renamed column"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t RENAME COLUMN tenant TO tenant_id"
$CLICKHOUSE_CLIENT -q "SELECT 'after rename', arraySort(groupArray(id)) FROM t"
$CLICKHOUSE_CLIENT -q "SHOW CREATE ROW POLICY p ON $DB.t" | sed "s/$DB/db/g"

echo "-- a type change is allowed"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t MODIFY COLUMN tenant_id UInt64"
$CLICKHOUSE_CLIENT -q "SELECT 'after modify', arraySort(groupArray(id)) FROM t"

echo "-- two renames in one statement"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t RENAME COLUMN tenant_id TO tenant, RENAME COLUMN note TO comment"
$CLICKHOUSE_CLIENT -q "SELECT 'after two renames', arraySort(groupArray(id)) FROM t"
$CLICKHOUSE_CLIENT -q "SHOW CREATE ROW POLICY p ON $DB.t" | sed "s/$DB/db/g"

echo "-- a subquery over another table keeps that table's column"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE allowed (tenant UInt32) ENGINE = MergeTree ORDER BY tenant;
INSERT INTO allowed VALUES (1);
CREATE TABLE t2 (id UInt32, tenant UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t2 VALUES (1, 1), (2, 2);
CREATE ROW POLICY psub ON $DB.t2 FOR SELECT USING tenant IN (SELECT tenant FROM allowed) TO CURRENT_USER;
ALTER TABLE t2 RENAME COLUMN tenant TO tenant_id;
SELECT 'subquery policy after rename', arraySort(groupArray(id)) FROM t2;
"
$CLICKHOUSE_CLIENT -q "SHOW CREATE ROW POLICY psub ON $DB.t2" | sed "s/$DB/db/g"

echo "-- IF EXISTS on a missing column stays a no-op even if a stale policy mentions it"
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY pstale ON $DB.t FOR SELECT USING gone = 1 TO CURRENT_USER"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t DROP COLUMN IF EXISTS gone"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t RENAME COLUMN IF EXISTS gone TO still_gone"
$CLICKHOUSE_CLIENT -q "SHOW CREATE ROW POLICY pstale ON $DB.t" | sed "s/$DB/db/g"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY pstale ON $DB.t"

echo "-- a database-wide policy cannot follow a rename on one table"
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY pdb ON $DB.* FOR SELECT USING tenant = 1 TO CURRENT_USER"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t RENAME COLUMN tenant TO tenant2" 2>&1 | grep -o "ALTER_OF_COLUMN_IS_FORBIDDEN" | head -1
$CLICKHOUSE_CLIENT -q "SELECT 'still readable', arraySort(groupArray(id)) FROM t"

$CLICKHOUSE_CLIENT --multiquery -q "
DROP ROW POLICY pdb ON $DB.*;
DROP ROW POLICY p ON $DB.t;
DROP ROW POLICY psub ON $DB.t2;
DROP TABLE t;
DROP TABLE t2;
DROP TABLE allowed;
"
