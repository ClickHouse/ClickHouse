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
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY pdb ON $DB.*"

echo "-- a table-qualified column is renamed too"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE tq (id UInt32, tenant UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO tq VALUES (1, 1), (2, 2);
CREATE ROW POLICY pq ON $DB.tq FOR SELECT USING tq.tenant = 1 AND $DB.tq.tenant = 1 TO CURRENT_USER;
ALTER TABLE tq RENAME COLUMN tenant TO tenant_id;
SELECT 'qualified policy after rename', arraySort(groupArray(id)) FROM tq;
"
$CLICKHOUSE_CLIENT -q "SHOW CREATE ROW POLICY pq ON $DB.tq" | sed "s/$DB/db/g"

echo "-- a subcolumn counts as a use of its column"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE tj (id UInt32, j JSON) ENGINE = MergeTree ORDER BY id;
INSERT INTO tj VALUES (1, '{\"user\":{\"name\":\"a\"}}'), (2, '{\"user\":{\"name\":\"b\"}}');
CREATE ROW POLICY pj ON $DB.tj FOR SELECT USING j.user.name = 'a' TO CURRENT_USER;
"
$CLICKHOUSE_CLIENT -q "ALTER TABLE tj DROP COLUMN j" 2>&1 | grep -o "ALTER_OF_COLUMN_IS_FORBIDDEN" | head -1
$CLICKHOUSE_CLIENT -q "ALTER TABLE tj RENAME COLUMN j TO data"
$CLICKHOUSE_CLIENT -q "SELECT 'subcolumn policy after rename', arraySort(groupArray(id)) FROM tj"
$CLICKHOUSE_CLIENT -q "SHOW CREATE ROW POLICY pj ON $DB.tj" | sed "s/$DB/db/g"

echo "-- dropping a Nested column used through its subcolumn is refused"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE tn (id UInt32, n Nested(x UInt32, y UInt32)) ENGINE = MergeTree ORDER BY id;
CREATE ROW POLICY pn ON $DB.tn FOR SELECT USING n.x[1] = 1 TO CURRENT_USER;
"
$CLICKHOUSE_CLIENT -q "ALTER TABLE tn DROP COLUMN n" 2>&1 | grep -o "ALTER_OF_COLUMN_IS_FORBIDDEN" | head -1
$CLICKHOUSE_CLIENT -q "ALTER TABLE tn DROP COLUMN n.y"

echo "-- a column read inside a SQL function body cannot be dropped or renamed"
UDF="${DB}_udf"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE FUNCTION $UDF AS (x) -> x = tenant;
CREATE TABLE tu (id UInt32, tenant UInt32, other UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO tu VALUES (1, 1, 0), (2, 2, 0);
CREATE ROW POLICY pu ON $DB.tu FOR SELECT USING $UDF(1) TO CURRENT_USER;
"
$CLICKHOUSE_CLIENT -q "ALTER TABLE tu DROP COLUMN tenant" 2>&1 | grep -o "ALTER_OF_COLUMN_IS_FORBIDDEN" | head -1
$CLICKHOUSE_CLIENT -q "ALTER TABLE tu RENAME COLUMN tenant TO tenant_id" 2>&1 | grep -o "ALTER_OF_COLUMN_IS_FORBIDDEN" | head -1
$CLICKHOUSE_CLIENT -q "ALTER TABLE tu RENAME COLUMN other TO other2"
$CLICKHOUSE_CLIENT -q "SELECT 'udf policy still works', arraySort(groupArray(id)) FROM tu"

$CLICKHOUSE_CLIENT --multiquery -q "
DROP ROW POLICY p ON $DB.t;
DROP ROW POLICY psub ON $DB.t2;
DROP ROW POLICY pq ON $DB.tq;
DROP ROW POLICY pj ON $DB.tj;
DROP ROW POLICY pn ON $DB.tn;
DROP ROW POLICY pu ON $DB.tu;
DROP FUNCTION $UDF;
DROP TABLE t;
DROP TABLE t2;
DROP TABLE allowed;
DROP TABLE tq;
DROP TABLE tj;
DROP TABLE tn;
DROP TABLE tu;
"
