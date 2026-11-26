#!/usr/bin/env bash
# a column that a row policy mentions cannot be dropped or renamed, everything else stays allowed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=$CLICKHOUSE_DATABASE
UDF="${DB}_udf"

refused() { $CLICKHOUSE_CLIENT -q "$1" 2>&1 | grep -o "$2" | head -1; }

$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE t (id UInt32, tenant UInt32, note String, extra UInt8, misc UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO t VALUES (1, 1, 'a', 0, 0), (2, 2, 'b', 0, 0), (3, 1, 'c', 0, 0);
CREATE ROW POLICY p ON $DB.t FOR SELECT USING tenant = 1 AND length(note) > 0 TO CURRENT_USER;
SELECT 'before', arraySort(groupArray(id)) FROM t;
"

echo "-- a column the policy mentions cannot be dropped or renamed"
refused "ALTER TABLE t DROP COLUMN tenant" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE t RENAME COLUMN note TO comment" ALTER_OF_COLUMN_IS_FORBIDDEN

echo "-- other columns are free"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t DROP COLUMN extra"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t RENAME COLUMN misc TO misc2"
$CLICKHOUSE_CLIENT -q "SELECT 'after', arraySort(groupArray(id)) FROM t"

echo "-- a type change and CLEAR keep the column, so they are allowed"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t MODIFY COLUMN tenant UInt64"
$CLICKHOUSE_CLIENT -q "SELECT 'after modify', arraySort(groupArray(id)) FROM t"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t CLEAR COLUMN tenant"
$CLICKHOUSE_CLIENT -q "SELECT 'after clear', arraySort(groupArray(id)) FROM t"
$CLICKHOUSE_CLIENT -q "INSERT INTO t (id, tenant, note) VALUES (4, 1, 'd')"
$CLICKHOUSE_CLIENT -q "SELECT 'after reinsert', arraySort(groupArray(id)) FROM t"

echo "-- IF EXISTS on a missing column stays a no-op even if a stale policy mentions it"
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY pstale ON $DB.t FOR SELECT USING gone = 1 TO CURRENT_USER"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t DROP COLUMN IF EXISTS gone"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t RENAME COLUMN IF EXISTS gone TO still_gone"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY pstale ON $DB.t"

echo "-- the names inside a subquery belong to the tables the subquery reads"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE allowed (tenant UInt32) ENGINE = MergeTree ORDER BY tenant;
INSERT INTO allowed VALUES (1);
CREATE TABLE t2 (id UInt32, tenant UInt32, other UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t2 VALUES (1, 1, 0), (2, 2, 0);
CREATE ROW POLICY psub ON $DB.t2 FOR SELECT USING id IN (SELECT tenant FROM allowed) TO CURRENT_USER;
"
refused "ALTER TABLE t2 DROP COLUMN id" ALTER_OF_COLUMN_IS_FORBIDDEN
$CLICKHOUSE_CLIENT -q "ALTER TABLE t2 RENAME COLUMN tenant TO tenant_id"
$CLICKHOUSE_CLIENT -q "SELECT 'subquery policy after rename', arraySort(groupArray(id)) FROM t2"

echo "-- table-qualified names count"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE tq (id UInt32, tenant UInt32, other UInt32) ENGINE = MergeTree ORDER BY id;
CREATE ROW POLICY pq ON $DB.tq FOR SELECT USING tq.tenant = 1 AND $DB.tq.tenant = 1 TO CURRENT_USER;
"
refused "ALTER TABLE tq DROP COLUMN tenant" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE tq RENAME COLUMN tenant TO tenant_id" ALTER_OF_COLUMN_IS_FORBIDDEN
$CLICKHOUSE_CLIENT -q "ALTER TABLE tq RENAME COLUMN other TO other2"

echo "-- a subcolumn counts as a mention of its column"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE tj (id UInt32, j JSON) ENGINE = MergeTree ORDER BY id;
CREATE ROW POLICY pj ON $DB.tj FOR SELECT USING j.user.name = 'a' TO CURRENT_USER;
"
refused "ALTER TABLE tj DROP COLUMN j" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE tj RENAME COLUMN j TO data" ALTER_OF_COLUMN_IS_FORBIDDEN

echo "-- so does a Nested child"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE tn (id UInt32, n Nested(x UInt32, y UInt32)) ENGINE = MergeTree ORDER BY id;
CREATE ROW POLICY pn ON $DB.tn FOR SELECT USING n.x[1] = 1 TO CURRENT_USER;
"
refused "ALTER TABLE tn DROP COLUMN n" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE tn RENAME COLUMN n.x TO n.z" ALTER_OF_COLUMN_IS_FORBIDDEN
$CLICKHOUSE_CLIENT -q "ALTER TABLE tn DROP COLUMN n.y"

echo "-- a column mentioned inside a SQL function body"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE FUNCTION $UDF AS (x) -> x = tenant;
CREATE TABLE tu (id UInt32, tenant UInt32, other UInt32) ENGINE = MergeTree ORDER BY id;
CREATE ROW POLICY pu ON $DB.tu FOR SELECT USING $UDF(1) TO CURRENT_USER;
"
refused "ALTER TABLE tu DROP COLUMN tenant" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE tu RENAME COLUMN tenant TO tenant_id" ALTER_OF_COLUMN_IS_FORBIDDEN
$CLICKHOUSE_CLIENT -q "ALTER TABLE tu RENAME COLUMN other TO other2"

echo "-- so is a SQL function passed by name as a lambda"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE tf (id UInt32, tenant UInt32) ENGINE = MergeTree ORDER BY id;
CREATE ROW POLICY pf ON $DB.tf FOR SELECT USING arrayExists($UDF, [1]) TO CURRENT_USER;
"
refused "ALTER TABLE tf DROP COLUMN tenant" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE tf RENAME COLUMN tenant TO tenant_id" ALTER_OF_COLUMN_IS_FORBIDDEN

echo "-- a lambda parameter is not a column, the body is still read"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE tlam (id UInt32, x UInt8, tenant UInt32, arr Array(UInt8)) ENGINE = MergeTree ORDER BY id;
INSERT INTO tlam VALUES (1, 0, 1, [1]), (2, 0, 2, [2]);
CREATE ROW POLICY plam ON $DB.tlam FOR SELECT USING arrayExists(x -> x = 1, arr) AND $UDF(id) TO CURRENT_USER;
"
$CLICKHOUSE_CLIENT -q "ALTER TABLE tlam DROP COLUMN x"
$CLICKHOUSE_CLIENT -q "SELECT 'lambda policy after drop', arraySort(groupArray(id)) FROM tlam"
refused "ALTER TABLE tlam DROP COLUMN arr" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE tlam RENAME COLUMN tenant TO tenant_id" ALTER_OF_COLUMN_IS_FORBIDDEN

echo "-- a name that starts with the table name can also be a column of that name"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE tt (id UInt32, tt Tuple(x UInt8)) ENGINE = MergeTree ORDER BY id;
CREATE ROW POLICY ptt ON $DB.tt FOR SELECT USING tt.x = 1 TO CURRENT_USER;
CREATE TABLE tl (id UInt32, \`tl.x\` UInt8) ENGINE = MergeTree ORDER BY id;
CREATE ROW POLICY ptl ON $DB.tl FOR SELECT USING tl.x = 1 TO CURRENT_USER;
"
refused "ALTER TABLE tt DROP COLUMN tt" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE tl RENAME COLUMN \`tl.x\` TO \`tl.y\`" ALTER_OF_COLUMN_IS_FORBIDDEN

echo "-- a read through an Alias applies the policies of the alias and of its target"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE tal (id UInt32, tenant UInt32, note String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE al ENGINE = Alias(tal);
CREATE ROW POLICY ptal ON $DB.tal FOR SELECT USING tenant = 1 TO CURRENT_USER;
CREATE ROW POLICY pal ON $DB.al FOR SELECT USING length(al.note) > 0 TO CURRENT_USER;
"
refused "ALTER TABLE al DROP COLUMN tenant" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE tal RENAME COLUMN note TO comment" ALTER_OF_COLUMN_IS_FORBIDDEN

echo "-- a policy of an Alias can read the target in a subquery, those names count"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE tal2 (id UInt32, tenant UInt32, note String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE al2 ENGINE = Alias(tal2);
CREATE ROW POLICY pal2 ON $DB.al2 FOR SELECT USING id IN (SELECT id FROM tal2 WHERE tenant = 1) TO CURRENT_USER;
"
refused "ALTER TABLE tal2 DROP COLUMN tenant" ALTER_OF_COLUMN_IS_FORBIDDEN
$CLICKHOUSE_CLIENT -q "ALTER TABLE tal2 RENAME COLUMN note TO comment"

echo "-- the policy pins an ALIAS column, the alias pins its base column"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE ta (id UInt32, tenant UInt32, tenant_alias UInt32 ALIAS tenant) ENGINE = MergeTree ORDER BY id;
INSERT INTO ta VALUES (1, 1), (2, 2);
CREATE ROW POLICY pa ON $DB.ta FOR SELECT USING tenant_alias = 1 TO CURRENT_USER;
"
refused "ALTER TABLE ta DROP COLUMN tenant_alias" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE ta DROP COLUMN tenant" ILLEGAL_COLUMN
$CLICKHOUSE_CLIENT -q "ALTER TABLE ta RENAME COLUMN tenant TO tenant2"
$CLICKHOUSE_CLIENT -q "SELECT 'alias policy after base rename', arraySort(groupArray(id)) FROM ta"

echo "-- a database-wide policy counts too"
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY pdb ON $DB.* FOR SELECT USING tenant = 1 TO CURRENT_USER"
refused "ALTER TABLE t RENAME COLUMN tenant TO tenant2" ALTER_OF_COLUMN_IS_FORBIDDEN
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY pdb ON $DB.*"

$CLICKHOUSE_CLIENT --multiquery -q "
DROP ROW POLICY p ON $DB.t;
DROP ROW POLICY psub ON $DB.t2;
DROP ROW POLICY pq ON $DB.tq;
DROP ROW POLICY pj ON $DB.tj;
DROP ROW POLICY pn ON $DB.tn;
DROP ROW POLICY pu ON $DB.tu;
DROP ROW POLICY pa ON $DB.ta;
DROP ROW POLICY pf ON $DB.tf;
DROP ROW POLICY plam ON $DB.tlam;
DROP ROW POLICY ptt ON $DB.tt;
DROP ROW POLICY ptl ON $DB.tl;
DROP ROW POLICY ptal ON $DB.tal;
DROP ROW POLICY pal ON $DB.al;
DROP ROW POLICY pal2 ON $DB.al2;
DROP FUNCTION $UDF;
DROP TABLE t;
DROP TABLE t2;
DROP TABLE allowed;
DROP TABLE tq;
DROP TABLE tj;
DROP TABLE tn;
DROP TABLE tu;
DROP TABLE ta;
DROP TABLE tf;
DROP TABLE tlam;
DROP TABLE tt;
DROP TABLE tl;
DROP TABLE al;
DROP TABLE tal;
DROP TABLE al2;
DROP TABLE tal2;
"
