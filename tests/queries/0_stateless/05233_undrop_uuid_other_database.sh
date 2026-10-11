#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database

# `UNDROP ... UUID` resolves the dropped-table queue entry by UUID alone, so the
# named database can differ from the one the table was dropped from. All
# database-dependent checks and the metadata move must act on the dropped
# table's own database: a guard on the named database used to pass and the
# later metadata-path lookup then threw LOGICAL_ERROR (aborting sanitizer
# builds), and the metadata file was moved on the named database's disk.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB_A="${CLICKHOUSE_DATABASE}_a"
DB_B="${CLICKHOUSE_DATABASE}_b"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB_A SYNC"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB_B SYNC"

echo 'undrop by UUID across databases restores into the dropped table database'
uuid=$($CLICKHOUSE_CLIENT --query "SELECT generateUUIDv4()")
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $DB_A ENGINE = Atomic"
$CLICKHOUSE_CLIENT -q "CREATE TABLE $DB_A.t UUID '$uuid' (id Int32) ENGINE = MergeTree() ORDER BY id"
$CLICKHOUSE_CLIENT -q "INSERT INTO $DB_A.t VALUES (1),(2),(3)"
$CLICKHOUSE_CLIENT -q "DROP TABLE $DB_A.t SETTINGS database_atomic_wait_for_drop_and_detach_synchronously = 0"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $DB_B ENGINE = Atomic"
$CLICKHOUSE_CLIENT -q "UNDROP TABLE $DB_B.t UUID '$uuid'"
$CLICKHOUSE_CLIENT -q "SELECT * FROM $DB_A.t ORDER BY id"
$CLICKHOUSE_CLIENT -q "DROP TABLE $DB_A.t SYNC"

echo 'undrop by UUID with a non-on-disk source database fails with UNKNOWN_TABLE'
uuid=$($CLICKHOUSE_CLIENT --query "SELECT generateUUIDv4()")
$CLICKHOUSE_CLIENT -q "CREATE TABLE $DB_A.t UUID '$uuid' (id Int32) ENGINE = MergeTree() ORDER BY id"
$CLICKHOUSE_CLIENT -q "DROP TABLE $DB_A.t SETTINGS database_atomic_wait_for_drop_and_detach_synchronously = 0"
$CLICKHOUSE_CLIENT -q "DROP DATABASE $DB_A"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $DB_A ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "UNDROP TABLE $DB_B.t UUID '$uuid';" 2>&1 | grep -Faq "UNKNOWN_TABLE" && echo OK

$CLICKHOUSE_CLIENT -q "DROP DATABASE $DB_A"
$CLICKHOUSE_CLIENT -q "DROP DATABASE $DB_B"
