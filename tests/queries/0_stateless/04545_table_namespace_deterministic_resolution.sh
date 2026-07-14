#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Table namespaces (experimental): deterministic name rules.
# a.b always means database.table; db.ns.table is a table path; USE db.ns scopes unqualified names.

DB=$CLICKHOUSE_DATABASE
# scope reads require the analyzer; pin it so the old-analyzer CI variant passes
CH="$CLICKHOUSE_CLIENT --allow_experimental_table_namespaces=1 --enable_analyzer=1"

$CH -m -q "
CREATE TABLE $DB.\`ns.t\` (x Int32) ENGINE = Memory;
CREATE TABLE $DB.\`ns.child.t2\` (x Int32) ENGINE = Memory;
CREATE TABLE $DB.t (x Int32) ENGINE = Memory;
INSERT INTO $DB.\`ns.t\` VALUES (1);
INSERT INTO $DB.t VALUES (100);
"

echo "-- flag off: multipart paths are syntax errors (master behavior)"
$CLICKHOUSE_CLIENT -q "SELECT * FROM $DB.ns.t" 2>&1 | grep -m1 -c "SYNTAX_ERROR\|Syntax error"
$CLICKHOUSE_CLIENT -q "USE $DB.ns" 2>&1 | grep -m1 -c "SYNTAX_ERROR\|Syntax error"

echo "-- flag on: three-part path addresses the dotted table"
$CH -q "SELECT * FROM $DB.ns.t"

echo "-- USE db.ns scopes unqualified names"
$CH -m -q "
USE $DB.ns;
SELECT * FROM t;
INSERT INTO t VALUES (2);
SELECT count() FROM t;
"
$CH -q "SELECT count() FROM $DB.\`ns.t\`"

echo "-- a two-part name always means database.table: no namespace fallback"
$CH -m -q "USE $DB; SELECT * FROM ns.t" 2>&1 | grep -m1 -c "UNKNOWN_DATABASE"

echo "-- deterministic even when a database with the same name exists"
DECOY="${CLICKHOUSE_DATABASE}_decoy"
$CH -m -q "
CREATE DATABASE $DECOY;
CREATE TABLE $DECOY.t (x Int32) ENGINE = Memory;
INSERT INTO $DECOY.t VALUES (42);
CREATE TABLE $DB.\`$DECOY.t\` (x Int32) ENGINE = Memory;
INSERT INTO $DB.\`$DECOY.t\` VALUES (777);
USE $DB;
SELECT * FROM $DECOY.t;
"
$CH -q "DROP DATABASE $DECOY"

echo "-- USE of a namespace that has no tables fails"
$CH -q "USE $DB.no_such_namespace" 2>&1 | grep -m1 -c "UNKNOWN_TABLE"

echo "-- introspection under the scope"
$CH -m -q "
USE $DB.ns;
EXISTS TABLE t;
EXISTS TABLE nope;
DESCRIBE TABLE t;
"
$CH -m -q "USE $DB.ns; SHOW CREATE TABLE t" | grep -m1 -c "ns.t"

echo "-- qualified column references over a table path"
$CH -q "SELECT $DB.ns.t.x FROM $DB.ns.t ORDER BY x"
$CH -q "SELECT ns.t.x FROM $DB.ns.t ORDER BY x"
$CH -q "SELECT a.x FROM $DB.ns.t AS a ORDER BY x"

echo "-- a stale prefix is inert once the setting is off"
$CH -m -q "
USE $DB.ns;
SET allow_experimental_table_namespaces = 0;
SELECT * FROM t;
"

echo "-- quoted components with literal dots cannot be smuggled into a path"
$CH -q "SELECT * FROM $DB.\`ns.child\`.t2" 2>&1 | grep -m1 -c "SYNTAX_ERROR\|Syntax error"

$CH -m -q "
DROP TABLE $DB.\`ns.t\`;
DROP TABLE $DB.\`ns.child.t2\`;
DROP TABLE $DB.t;
"
