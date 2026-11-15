#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database

# `UNDROP ... UUID` finds the dropped table by UUID alone, so the named database can differ
# from the one the table was dropped from. This is not supported and must throw `UNKNOWN_TABLE`
# (it used to throw `LOGICAL_ERROR` when the original database was re-created as `Memory`).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB_A="${CLICKHOUSE_DATABASE}_a"
DB_B="${CLICKHOUSE_DATABASE}_b"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB_A SYNC"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB_B SYNC"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $DB_A ENGINE = Atomic"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $DB_B ENGINE = Atomic"

echo 'undrop by UUID into a different database'
uuid=$($CLICKHOUSE_CLIENT --query "SELECT generateUUIDv4()")
$CLICKHOUSE_CLIENT -q "CREATE TABLE $DB_A.t UUID '$uuid' (id Int32) ENGINE = MergeTree() ORDER BY id"
$CLICKHOUSE_CLIENT -q "INSERT INTO $DB_A.t VALUES (1),(2),(3)"
$CLICKHOUSE_CLIENT -q "DROP TABLE $DB_A.t SETTINGS database_atomic_wait_for_drop_and_detach_synchronously = 0"
$CLICKHOUSE_CLIENT -q "UNDROP TABLE $DB_B.t UUID '$uuid'" 2>&1 | grep -Fq "UNKNOWN_TABLE" && echo OK
$CLICKHOUSE_CLIENT -q "UNDROP TABLE $DB_A.t UUID '$uuid'"
$CLICKHOUSE_CLIENT -q "SELECT * FROM $DB_A.t ORDER BY id"
$CLICKHOUSE_CLIENT -q "DROP TABLE $DB_A.t SYNC"

echo 'undrop by UUID when the original database was re-created as Memory'
uuid=$($CLICKHOUSE_CLIENT --query "SELECT generateUUIDv4()")
$CLICKHOUSE_CLIENT -q "CREATE TABLE $DB_A.t UUID '$uuid' (id Int32) ENGINE = MergeTree() ORDER BY id"
$CLICKHOUSE_CLIENT -q "DROP TABLE $DB_A.t SETTINGS database_atomic_wait_for_drop_and_detach_synchronously = 0"
$CLICKHOUSE_CLIENT -q "DROP DATABASE $DB_A"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $DB_A ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "UNDROP TABLE $DB_B.t UUID '$uuid'" 2>&1 | grep -Fq "UNKNOWN_TABLE" && echo OK
$CLICKHOUSE_CLIENT -q "UNDROP TABLE $DB_A.t UUID '$uuid'" 2>&1 | grep -Fq "UNKNOWN_TABLE" && echo OK

$CLICKHOUSE_CLIENT -q "DROP DATABASE $DB_A"
$CLICKHOUSE_CLIENT -q "DROP DATABASE $DB_B"
