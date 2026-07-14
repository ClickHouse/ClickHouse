#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=$CLICKHOUSE_DATABASE

$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE \`ns.alpha\` (x UInt8) ENGINE = Memory;
    CREATE TABLE \`ns.beta\` (x UInt8) ENGINE = Memory;
    CREATE TABLE \`ns.sub.gamma\` (x UInt8) ENGINE = Memory;
    CREATE TABLE \`other.delta\` (x UInt8) ENGINE = Memory;
    CREATE TABLE plain (x UInt8) ENGINE = Memory;
"

echo "-- SHOW TABLES under a namespace: relative names, direct children only"
$CLICKHOUSE_CLIENT -m -q "USE $db.ns; SHOW TABLES"

echo "-- LIKE applies to the relative name"
$CLICKHOUSE_CLIENT -m -q "USE $db.ns; SHOW TABLES LIKE 'al%'"

echo "-- WHERE applies to the relative name, same as LIKE"
$CLICKHOUSE_CLIENT -m -q "USE $db.ns; SHOW TABLES WHERE name = 'alpha'"

echo "-- WHERE can still use other table columns"
$CLICKHOUSE_CLIENT -m -q "USE $db.ns; SHOW TABLES WHERE engine = 'Memory'"

echo "-- SHOW TABLES FROM database.namespace"
$CLICKHOUSE_CLIENT -q "SHOW TABLES FROM $db.ns"

echo "-- nested namespace"
$CLICKHOUSE_CLIENT -q "SHOW TABLES FROM $db.ns.sub"

echo "-- FROM with a missing namespace is rejected like USE"
$CLICKHOUSE_CLIENT -q "SHOW TABLES FROM $db.no_such_namespace" 2>&1 | grep -m1 -c "UNKNOWN_TABLE"

echo "-- dictionaries have no namespaces"
$CLICKHOUSE_CLIENT -q "SHOW DICTIONARIES FROM $db.ns" 2>&1 | grep -m1 -c "UNKNOWN_DATABASE"

echo "-- without a namespace the full names are shown"
$CLICKHOUSE_CLIENT -q "SHOW TABLES FROM $db" | sort
