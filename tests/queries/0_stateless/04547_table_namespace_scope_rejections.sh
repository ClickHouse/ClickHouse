#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# While a table namespace is selected (USE db.ns), statements that do not honor the
# scope are rejected instead of silently targeting the database (experimental).

DB=$CLICKHOUSE_DATABASE
# scope reads require the analyzer; pin it so the old-analyzer CI variant passes
CH="$CLICKHOUSE_CLIENT --allow_experimental_table_namespaces=1 --enable_analyzer=1"

$CH -m -q "
CREATE TABLE $DB.\`ns.t\` (x Int32) ENGINE = Memory;
INSERT INTO $DB.\`ns.t\` VALUES (1);
"

reject() { $CH -m -q "USE $DB.ns; $1" 2>&1 | grep -m1 -c "NOT_IMPLEMENTED"; }

echo "-- DDL and management statements are rejected under a namespace scope"
reject "CREATE TABLE oops (x Int32) ENGINE = Memory"
reject "DROP TABLE t"
reject "ALTER TABLE t ADD COLUMN y Int32"
reject "RENAME TABLE t TO t2"
reject "TRUNCATE TABLE t"
reject "OPTIMIZE TABLE t"
reject "CREATE VIEW v AS SELECT * FROM t"
reject "GRANT SELECT ON *.* TO CURRENT_USER"
reject "SYSTEM STOP MERGES t"
reject "CHECK TABLE t"
reject "SHOW ROW POLICIES ON t"
reject "SHOW ROW POLICIES ON *"
reject "SHOW CREATE ROW POLICY nopol ON t"
reject "CREATE TABLE oops ON CLUSTER default (x Int32) ENGINE = Memory"
reject "BACKUP TABLE t TO Null"

echo "-- the scope requires the analyzer, entering and staying"
$CLICKHOUSE_CLIENT --allow_experimental_table_namespaces=1 --enable_analyzer=0 -q "USE $DB.ns" 2>&1 | grep -m1 -c "SUPPORT_IS_DISABLED"
$CH -m -q "USE $DB.ns; SET enable_analyzer = 0; SELECT * FROM t" 2>&1 | grep -m1 -c "SUPPORT_IS_DISABLED"

echo "-- reads, writes and introspection keep working under the scope"
$CH -m -q "
USE $DB.ns;
SELECT * FROM t;
INSERT INTO t VALUES (2);
SELECT count() FROM t;
EXISTS TABLE t;
SET max_threads = 1;
"
$CH -m -q "USE $DB.ns; DESCRIBE TABLE t" | cut -f1,2
$CH -m -q "USE $DB.ns; EXPLAIN SYNTAX SELECT 1" | head -1
$CH -m -q "USE $DB.ns; SHOW ROW POLICIES ON $DB.\`ns.t\`" | wc -l

echo "-- escapes through table functions and views are closed"
$CH -m -q "USE $DB.ns; SELECT sum(x) FROM (SELECT * FROM loop(t) LIMIT 2)"
reject "SELECT * FROM merge('^t\$')"
$CH -q "CREATE VIEW $DB.\`ns.v\` AS SELECT {p:Int32} AS p"
reject "SELECT * FROM v(p = 1)"
$CH -q "DROP VIEW $DB.\`ns.v\`"

echo "-- SHOW subqueries resolve under the scope, never against the parent"
$CH -m -q "
CREATE TABLE $DB.probe (x Int32) ENGINE = Memory;
CREATE TABLE $DB.\`ns.probe\` (x Int32) ENGINE = Memory;
INSERT INTO $DB.\`ns.probe\` VALUES (1);
"
$CH -m -q "USE $DB.ns; SHOW TABLES WHERE name = 'ns.probe' AND exists(SELECT * FROM probe)"
$CH -m -q "USE $DB.ns; SHOW TABLES FROM $DB WHERE name = 'probe' AND exists(SELECT * FROM probe)"
$CH -m -q "USE $DB.ns; SHOW TABLES LIMIT (SELECT count() FROM probe)"
$CH -m -q "USE $DB.ns; SHOW COLUMNS FROM t LIMIT (SELECT count() FROM probe)" | cut -f1,2
$CH -m -q "
DROP TABLE $DB.probe;
DROP TABLE $DB.\`ns.probe\`;
"

echo "-- outside the scope, DDL over an explicit path works and is deterministic"
$CH -m -q "
CREATE TABLE $DB.ns2.created_via_path (x Int32) ENGINE = Memory;
SHOW TABLES FROM $DB.ns2;
DROP TABLE $DB.ns2.created_via_path;
"

$CH -q "DROP TABLE $DB.\`ns.t\`"
