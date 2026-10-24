#!/usr/bin/env bash
# Tags: no-replicated-database
# RESTORE raises the metadata version of an existing ReplicatedMergeTree table with an ALTER_METADATA log entry.
# Column comments are not replicated through ZooKeeper, so the entry must keep the comments of the restoring replica,
# otherwise processing it would erase them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BACKUP_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT -q "CREATE TABLE src (id UInt64, x UInt64 COMMENT 'local comment') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/src', 'r1') ORDER BY id"
$CLICKHOUSE_CLIENT -q "ALTER TABLE src ADD COLUMN tmp UInt8"
$CLICKHOUSE_CLIENT -q "ALTER TABLE src DROP COLUMN tmp"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE src TO Disk('backups', '$BACKUP_NAME') FORMAT Null"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t (id UInt64, x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r1') ORDER BY id"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t COMMENT COLUMN x 'local comment'"

$CLICKHOUSE_CLIENT -q "RESTORE TABLE src AS t FROM Disk('backups', '$BACKUP_NAME') SETTINGS allow_different_table_def = 1 FORMAT Null"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA t"

$CLICKHOUSE_CLIENT -q "SELECT metadata_version FROM system.tables WHERE database = currentDatabase() AND name = 't'"
$CLICKHOUSE_CLIENT -q "SELECT name, comment FROM system.columns WHERE database = currentDatabase() AND table = 't' ORDER BY name"

$CLICKHOUSE_CLIENT -q "DROP TABLE src SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE t SYNC"
