#!/usr/bin/env bash
# a column that a row policy mentions cannot be dropped, renamed or get a new type, everything else stays allowed

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

echo "-- a new type of a column the policy mentions is refused, the same type and CLEAR are allowed"
refused "ALTER TABLE t MODIFY COLUMN tenant String" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE t MODIFY COLUMN tenant UInt64" ALTER_OF_COLUMN_IS_FORBIDDEN
$CLICKHOUSE_CLIENT -q "ALTER TABLE t MODIFY COLUMN tenant UInt32 CODEC(ZSTD)"
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
CREATE ROW POLICY psub ON $DB.t2 FOR SELECT USING id IN (SELECT tenant FROM $DB.allowed) TO CURRENT_USER;
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

echo "-- a name binds as written first, exact column or subcolumn, the qualifier is removed only after that"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE tt (id UInt32, x UInt8, tt Tuple(x UInt8)) ENGINE = MergeTree ORDER BY id;
CREATE ROW POLICY ptt ON $DB.tt FOR SELECT USING tt.x = 1 TO CURRENT_USER;
CREATE TABLE tl (id UInt32, x UInt8, \`tl.x\` UInt8, \`$DB.tl.x\` UInt8) ENGINE = MergeTree ORDER BY id;
CREATE ROW POLICY ptl ON $DB.tl FOR SELECT USING tl.x = 1 AND $DB.tl.x = 1 TO CURRENT_USER;
"
refused "ALTER TABLE tt DROP COLUMN tt" ALTER_OF_COLUMN_IS_FORBIDDEN
$CLICKHOUSE_CLIENT -q "ALTER TABLE tt DROP COLUMN x"
refused "ALTER TABLE tl RENAME COLUMN \`tl.x\` TO \`tl.y\`" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE tl DROP COLUMN \`$DB.tl.x\`" ALTER_OF_COLUMN_IS_FORBIDDEN
$CLICKHOUSE_CLIENT -q "ALTER TABLE tl DROP COLUMN x"

echo "-- a column of its own with a dot in the name is not a subcolumn of the shorter name"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE td (id UInt32, a UInt32, \`a.b\` UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO td VALUES (1, 0, 1), (2, 0, 2);
CREATE ROW POLICY pd ON $DB.td FOR SELECT USING \`a.b\` = 1 TO CURRENT_USER;
"
$CLICKHOUSE_CLIENT -q "ALTER TABLE td RENAME COLUMN a TO a2"
$CLICKHOUSE_CLIENT -q "ALTER TABLE td DROP COLUMN a2"
$CLICKHOUSE_CLIENT -q "SELECT 'dotted column policy after alters', arraySort(groupArray(id)) FROM td"
refused "ALTER TABLE td DROP COLUMN \`a.b\`" ALTER_OF_COLUMN_IS_FORBIDDEN

echo "-- a subquery over a same-named table of another database reads that table"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE DATABASE ${DB}_2;
CREATE TABLE ${DB}_2.ts (tenant UInt32) ENGINE = MergeTree ORDER BY tenant;
CREATE TABLE ts (id UInt32, tenant UInt32) ENGINE = MergeTree ORDER BY id;
CREATE ROW POLICY ps ON $DB.ts FOR SELECT USING id IN (SELECT tenant FROM ${DB}_2.ts) TO CURRENT_USER;
"
$CLICKHOUSE_CLIENT -q "ALTER TABLE ts RENAME COLUMN tenant TO tenant_id"

echo "-- a column named like a SQL function is the column, the function body is not read"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE tsame (id UInt32, $UDF UInt32, tenant UInt32) ENGINE = MergeTree ORDER BY id;
CREATE ROW POLICY psame ON $DB.tsame FOR SELECT USING $UDF = 1 TO CURRENT_USER;
"
$CLICKHOUSE_CLIENT -q "ALTER TABLE tsame DROP COLUMN tenant"
refused "ALTER TABLE tsame DROP COLUMN $UDF" ALTER_OF_COLUMN_IS_FORBIDDEN

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

echo "-- a name in that subquery can have the qualifier of the target or of its alias"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE tal3 (id UInt32, tenant UInt32, note String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE al3 ENGINE = Alias(tal3);
CREATE ROW POLICY pal3 ON $DB.al3 FOR SELECT USING id IN (SELECT q.id FROM tal3 AS q WHERE q.tenant = 1)
    AND id IN (SELECT id FROM tal3 WHERE length(tal3.note) > 0) TO CURRENT_USER;
"
refused "ALTER TABLE tal3 DROP COLUMN tenant" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE tal3 RENAME COLUMN note TO comment" ALTER_OF_COLUMN_IS_FORBIDDEN

echo "-- the names of a subquery count only when it, or a subquery around it, reads the target"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE tal4 (id UInt32, tenant UInt32, note String, extra UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO tal4 VALUES (1, 1, 'a', 1), (2, 2, 'b', 1), (3, 1, 'c', 5);
CREATE TABLE other4 (id UInt32, note String) ENGINE = MergeTree ORDER BY id;
INSERT INTO other4 VALUES (1, 'x'), (2, 'y'), (3, 'z');
CREATE TABLE al4 ENGINE = Alias(tal4);
CREATE ROW POLICY pal4 ON $DB.al4 FOR SELECT USING id IN (SELECT id FROM $DB.other4 WHERE note != '' AND id IN (SELECT q.id FROM $DB.tal4 AS q WHERE q.tenant = 1))
    AND id IN (SELECT q.id FROM $DB.tal4 AS q WHERE EXISTS (SELECT 1 FROM $DB.other4 AS o WHERE o.id = q.extra)) TO CURRENT_USER;
"
$CLICKHOUSE_CLIENT -q "ALTER TABLE tal4 DROP COLUMN note"
refused "ALTER TABLE tal4 DROP COLUMN tenant" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE tal4 DROP COLUMN extra" ALTER_OF_COLUMN_IS_FORBIDDEN
$CLICKHOUSE_CLIENT -q "SELECT 'nested subquery policy after drop', arraySort(groupArray(id)) FROM al4 SETTINGS enable_parallel_replicas = 0"

echo "-- a derived table or a CTE that reads the target qualifies the names of the target with its own name"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE tal5 (id UInt32, tenant UInt32, note String, extra UInt32, flag UInt32, misc UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO tal5 VALUES (1, 1, 'a', 1, 1, 0), (2, 2, 'b', 1, 1, 0), (3, 1, 'c', 1, 1, 0);
CREATE TABLE other5 (id UInt32, misc UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO other5 VALUES (1, 1), (2, 1), (3, 1);
CREATE TABLE al5 ENGINE = Alias(tal5);
CREATE ROW POLICY pal5 ON $DB.al5 FOR SELECT USING id IN (SELECT q.id FROM (SELECT * FROM $DB.tal5) AS q WHERE q.tenant = 1)
    AND id IN (WITH c AS (SELECT * FROM $DB.tal5) SELECT c.id FROM c WHERE length(c.note) > 0)
    AND id IN (WITH c AS (SELECT * FROM $DB.tal5) SELECT r.id FROM c AS r JOIN (SELECT * FROM $DB.other5) AS o ON r.id = o.id WHERE r.extra = 1 AND o.misc = 1)
    AND id IN (WITH c AS (SELECT * FROM $DB.tal5) SELECT s.id FROM (SELECT * FROM c) AS s WHERE s.flag = 1) TO CURRENT_USER;
"
refused "ALTER TABLE tal5 DROP COLUMN tenant" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE tal5 RENAME COLUMN note TO comment" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE tal5 DROP COLUMN extra" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE tal5 DROP COLUMN flag" ALTER_OF_COLUMN_IS_FORBIDDEN
$CLICKHOUSE_CLIENT -q "ALTER TABLE tal5 DROP COLUMN misc"
$CLICKHOUSE_CLIENT -q "SELECT 'derived table policy after drop', arraySort(groupArray(id)) FROM al5 SETTINGS enable_parallel_replicas = 0"

echo "-- so does a table function that reads the target"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE tal6 (id UInt32, tenant UInt32, misc UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO tal6 VALUES (1, 1, 0), (2, 2, 0), (3, 1, 0);
CREATE TABLE al6 ENGINE = Alias(tal6);
CREATE ROW POLICY pal6 ON $DB.al6 FOR SELECT USING id IN (SELECT q.id FROM view(SELECT * FROM $DB.tal6) AS q WHERE q.tenant = 1) TO CURRENT_USER;
"
refused "ALTER TABLE tal6 DROP COLUMN tenant" ALTER_OF_COLUMN_IS_FORBIDDEN
$CLICKHOUSE_CLIENT -q "ALTER TABLE tal6 DROP COLUMN misc"
$CLICKHOUSE_CLIENT -q "SELECT 'table function policy after drop', arraySort(groupArray(id)) FROM al6 SETTINGS enable_parallel_replicas = 0"

echo "-- a qualifier binds in its own subquery, an alias of another table there hides the policy table and the outer aliases"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE tal7 (id UInt32, tenant UInt32, flag UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO tal7 VALUES (1, 1, 1), (2, 2, 1), (3, 1, 1);
CREATE TABLE other7 (id UInt32, tenant UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO other7 VALUES (1, 1), (2, 1), (3, 2);
CREATE TABLE al7 ENGINE = Alias(tal7);
CREATE ROW POLICY pal7 ON $DB.al7 FOR SELECT USING id IN (SELECT r.id FROM $DB.tal7 AS r INNER JOIN $DB.other7 AS al7 ON r.id = al7.id WHERE al7.tenant = 1 AND r.flag = 1)
    AND id IN (SELECT q.id FROM $DB.tal7 AS q WHERE q.id IN (SELECT q.id FROM $DB.other7 AS q WHERE q.tenant = 1)) TO CURRENT_USER;
"
refused "ALTER TABLE tal7 DROP COLUMN flag" ALTER_OF_COLUMN_IS_FORBIDDEN
$CLICKHOUSE_CLIENT -q "ALTER TABLE tal7 RENAME COLUMN tenant TO tenant_id"
$CLICKHOUSE_CLIENT -q "SELECT 'scoped alias policy after rename', arraySort(groupArray(id)) FROM al7 SETTINGS enable_parallel_replicas = 0"

echo "-- a table qualifier that is also a column name is removed when the subcolumn does not exist"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE tc (id UInt32, tc UInt8, tenant UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO tc VALUES (1, 0, 1), (2, 0, 2);
CREATE ROW POLICY pc ON $DB.tc FOR SELECT USING tc.tenant = 1 TO CURRENT_USER;
"
refused "ALTER TABLE tc DROP COLUMN tenant" ALTER_OF_COLUMN_IS_FORBIDDEN
$CLICKHOUSE_CLIENT -q "ALTER TABLE tc DROP COLUMN tc"
$CLICKHOUSE_CLIENT -q "SELECT 'qualifier column policy after drop', arraySort(groupArray(id)) FROM tc"

echo "-- a flattened Nested group reads all of its children"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE tng (id UInt32, n Nested(x UInt32, y UInt32), other UInt8) ENGINE = MergeTree ORDER BY id;
CREATE ROW POLICY png ON $DB.tng FOR SELECT USING notEmpty(n) TO CURRENT_USER;
"
refused "ALTER TABLE tng DROP COLUMN n" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE tng DROP COLUMN n.y" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE tng RENAME COLUMN n.x TO n.z" ALTER_OF_COLUMN_IS_FORBIDDEN
$CLICKHOUSE_CLIENT -q "ALTER TABLE tng DROP COLUMN other"

echo "-- a column read as a subcolumn, through a function or next to a scalar subquery cannot get a new type"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE tm (id UInt32, tp Tuple(a UInt8, b UInt8), n Nullable(UInt8), k UInt32, u Tuple(a UInt8, b UInt8), free UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO tm VALUES (1, (1, 1), 1, 1, (1, 1), 0), (2, (1, 2), 2, 1, (1, 2), 0);
CREATE ROW POLICY pm ON $DB.tm FOR SELECT USING tm.tp.b = 1 AND NOT n.null AND k = 1 AND (SELECT 1) = 1 AND tupleElement(u, 'b') = 1 TO CURRENT_USER;
"
refused "ALTER TABLE tm MODIFY COLUMN tp Tuple(a UInt8)" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE tm MODIFY COLUMN tp Tuple(a UInt8, c UInt8)" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE tm MODIFY COLUMN tp Tuple(a UInt8, b UInt8, c UInt8)" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE tm MODIFY COLUMN n UInt8 DEFAULT 0" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE tm MODIFY COLUMN k String" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE tm MODIFY COLUMN u Tuple(a UInt8)" ALTER_OF_COLUMN_IS_FORBIDDEN
$CLICKHOUSE_CLIENT -q "ALTER TABLE tm MODIFY COLUMN n COMMENT 'kept'"
$CLICKHOUSE_CLIENT -q "ALTER TABLE tm MODIFY COLUMN free String"
$CLICKHOUSE_CLIENT -q "SELECT 'modified columns policy', arraySort(groupArray(id)) FROM tm"

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

echo "-- an ALIAS column the policy reads pins the type of its base columns"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE tac (id UInt32, n UInt32, a UInt8 ALIAS n = 1, b UInt8 ALIAS a) ENGINE = MergeTree ORDER BY id;
INSERT INTO tac (id, n) VALUES (1, 1), (2, 2);
CREATE ROW POLICY pac ON $DB.tac FOR SELECT USING b TO CURRENT_USER;
"
refused "ALTER TABLE tac MODIFY COLUMN n String" ALTER_OF_COLUMN_IS_FORBIDDEN
$CLICKHOUSE_CLIENT -q "ALTER TABLE tac MODIFY COLUMN n UInt32 CODEC(ZSTD)"
$CLICKHOUSE_CLIENT -q "SELECT 'alias over a modified column', arraySort(groupArray(id)) FROM tac"

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
DROP ROW POLICY pac ON $DB.tac;
DROP ROW POLICY pf ON $DB.tf;
DROP ROW POLICY plam ON $DB.tlam;
DROP ROW POLICY ptt ON $DB.tt;
DROP ROW POLICY ptl ON $DB.tl;
DROP ROW POLICY pd ON $DB.td;
DROP ROW POLICY ps ON $DB.ts;
DROP ROW POLICY psame ON $DB.tsame;
DROP ROW POLICY ptal ON $DB.tal;
DROP ROW POLICY pal ON $DB.al;
DROP ROW POLICY pal2 ON $DB.al2;
DROP ROW POLICY pal3 ON $DB.al3;
DROP ROW POLICY pal4 ON $DB.al4;
DROP ROW POLICY pal5 ON $DB.al5;
DROP ROW POLICY pal6 ON $DB.al6;
DROP ROW POLICY pal7 ON $DB.al7;
DROP ROW POLICY pc ON $DB.tc;
DROP ROW POLICY png ON $DB.tng;
DROP ROW POLICY pm ON $DB.tm;
DROP FUNCTION $UDF;
DROP TABLE t;
DROP TABLE t2;
DROP TABLE allowed;
DROP TABLE tq;
DROP TABLE tj;
DROP TABLE tn;
DROP TABLE tu;
DROP TABLE ta;
DROP TABLE tac;
DROP TABLE tf;
DROP TABLE tlam;
DROP TABLE tt;
DROP TABLE tl;
DROP TABLE td;
DROP TABLE ts;
DROP DATABASE ${DB}_2;
DROP TABLE tsame;
DROP TABLE al;
DROP TABLE tal;
DROP TABLE al2;
DROP TABLE tal2;
DROP TABLE al3;
DROP TABLE tal3;
DROP TABLE al4;
DROP TABLE tal4;
DROP TABLE other4;
DROP TABLE al5;
DROP TABLE tal5;
DROP TABLE other5;
DROP TABLE al6;
DROP TABLE tal6;
DROP TABLE al7;
DROP TABLE tal7;
DROP TABLE other7;
DROP TABLE tc;
DROP TABLE tng;
DROP TABLE tm;
"
