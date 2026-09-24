#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# SHOW TABLES over table namespaces (experimental).

DB=$CLICKHOUSE_DATABASE
# scope reads require the analyzer; pin it so the old-analyzer CI variant passes
CH="$CLICKHOUSE_CLIENT --allow_experimental_table_namespaces=1 --enable_analyzer=1"

$CH -m -q "
CREATE TABLE $DB.\`ns.alpha\` (x Int32) ENGINE = Memory;
CREATE TABLE $DB.\`ns.beta\` (x Int32) ENGINE = Memory;
CREATE TABLE $DB.\`ns.child.gamma\` (x Int32) ENGINE = Memory;
CREATE TABLE $DB.plain (x Int32) ENGINE = Memory;
"

echo "-- flag off: multipart FROM is a syntax error (master behavior)"
$CLICKHOUSE_CLIENT -q "SHOW TABLES FROM $DB.ns" 2>&1 | grep -m1 -c "SYNTAX_ERROR\|Syntax error"

echo "-- direct children of the namespace, shown as stored"
$CH -q "SHOW TABLES FROM $DB.ns"

echo "-- LIKE applies to the stored name"
$CH -q "SHOW TABLES FROM $DB.ns LIKE 'ns.al%'"

echo "-- FULL keeps the engine column"
$CH -q "SHOW FULL TABLES FROM $DB.ns LIKE 'ns.beta'"

echo "-- nested namespace"
$CH -q "SHOW TABLES FROM $DB.ns.child"

echo "-- session scope applies when FROM is omitted"
$CH -m -q "USE $DB.ns; SHOW TABLES"

echo "-- the plain database listing is unchanged"
$CH -q "SHOW TABLES FROM $DB"

echo "-- a namespace with no tables fails like USE does"
$CH -q "SHOW TABLES FROM $DB.no_such_namespace" 2>&1 | grep -m1 -c "UNKNOWN_TABLE"

echo "-- dictionaries have no namespaces"
$CH -q "SHOW DICTIONARIES FROM $DB.ns" 2>&1 | grep -m1 -c "UNKNOWN_DATABASE"

$CH -m -q "
DROP TABLE $DB.\`ns.alpha\`;
DROP TABLE $DB.\`ns.beta\`;
DROP TABLE $DB.\`ns.child.gamma\`;
DROP TABLE $DB.plain;
"
