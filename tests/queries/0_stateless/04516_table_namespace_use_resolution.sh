#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=$CLICKHOUSE_DATABASE

$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE \`ns.t\` (x UInt32) ENGINE = MergeTree ORDER BY x;
    INSERT INTO \`ns.t\` VALUES (1), (2), (3);
    CREATE TABLE \`ns.sub.t2\` (y String) ENGINE = Memory;
    CREATE TABLE plain (z UInt8) ENGINE = Memory;
"

echo "-- unqualified name resolves under the selected namespace"
$CLICKHOUSE_CLIENT -m -q "USE $db.ns; SELECT count() FROM t"

echo "-- a dotted relative name resolves under the namespace too"
$CLICKHOUSE_CLIENT -m -q "USE $db.ns; SELECT count() FROM \`sub.t2\`"

echo "-- EXISTS under the namespace"
$CLICKHOUSE_CLIENT -m -q "USE $db.ns; EXISTS TABLE t"

echo "-- two-part name with a non-database qualifier resolves inside the current database"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM ns.t"

echo "-- an existing database always wins over the namespace interpretation"
$CLICKHOUSE_CLIENT -m -q "
    CREATE DATABASE IF NOT EXISTS ${db}_real;
    CREATE TABLE ${db}_real.victim (x UInt32) ENGINE = Memory;
    INSERT INTO ${db}_real.victim VALUES (42);
    CREATE TABLE \`${db}_real.victim\` (x UInt32) ENGINE = Memory;
    SELECT x FROM ${db}_real.victim;
"

echo "-- USE of a namespace with no tables is rejected"
$CLICKHOUSE_CLIENT -q "USE $db.no_such_namespace" 2>&1 | grep -m1 -c "UNKNOWN_TABLE"

echo "-- the protocol default database validates the namespace the same way"
${CLICKHOUSE_CURL} -sS -H "X-ClickHouse-Database: $db.no_such_namespace" "${CLICKHOUSE_URL}" -d 'SELECT 1' | grep -m1 -c "UNKNOWN_TABLE"

echo "-- CREATE under the namespace lands inside it"
$CLICKHOUSE_CLIENT -m -q "USE $db.ns; CREATE TABLE created (a Int8) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "EXISTS TABLE $db.\`ns.created\`"

echo "-- CREATE with a two-part name and unknown database must fail, not create a dotted table"
$CLICKHOUSE_CLIENT -q "CREATE TABLE no_such_db_$db.t (a Int8) ENGINE = Memory" 2>&1 | grep -m1 -c "UNKNOWN_DATABASE"

echo "-- CREATE AS resolves its source under the namespace"
$CLICKHOUSE_CLIENT -m -q "USE $db.ns; CREATE TABLE copied AS t; SELECT count() FROM copied"

echo "-- materialized view target with a namespace path"
$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE \`ns.sink\` (z UInt8) ENGINE = Memory;
    CREATE MATERIALIZED VIEW mv_ns_target TO ns.sink AS SELECT z FROM plain;
    INSERT INTO plain VALUES (7);
    SELECT z FROM \`ns.sink\`;
"

echo "-- a quoted component with a literal dot cannot be part of a longer path"
$CLICKHOUSE_CLIENT -q "SELECT * FROM $db.\`a.b\`.t" 2>&1 | grep -m1 -c "Syntax error"
$CLICKHOUSE_CLIENT -q "USE $db.\`a.b\`" 2>&1 | grep -m1 -c "Syntax error"

echo "-- temporary table takes precedence over the namespace"
$CLICKHOUSE_CLIENT -m -q "
    USE $db.ns;
    CREATE TEMPORARY TABLE t (name String);
    INSERT INTO t VALUES ('temp');
    SELECT name FROM t;
"

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${db}_real"
