#!/usr/bin/env bash
# Tags: no-replicated-database
# The metadata versions saved by different hosts of one backup can differ while the structure is the same. When the
# higher one is restored first, a replica restoring the lower one which lags behind on the ALTER_METADATA entry of that
# restore has the structure of the table at the version in ZooKeeper, so it gets that version instead of an error.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BACKUP_LOW="${CLICKHOUSE_TEST_UNIQUE_NAME}_low"
BACKUP_HIGH="${CLICKHOUSE_TEST_UNIQUE_NAME}_high"

create_tables() {
    $CLICKHOUSE_CLIENT -q "CREATE TABLE t_r1 (id UInt64, x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r1') ORDER BY id"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE t_r2 (id UInt64, x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r2') ORDER BY id"
}

create_tables
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_r1 ADD COLUMN tmp UInt8 SETTINGS alter_sync = 2"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_r1 DROP COLUMN tmp SETTINGS alter_sync = 2"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE t_r2 TO Disk('backups', '$BACKUP_LOW') SETTINGS structure_only = 1 FORMAT Null"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_r1 ADD COLUMN tmp UInt8 SETTINGS alter_sync = 2"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_r1 DROP COLUMN tmp SETTINGS alter_sync = 2"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE t_r1 TO Disk('backups', '$BACKUP_HIGH') SETTINGS structure_only = 1 FORMAT Null"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_r1 SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_r2 SYNC"

create_tables
$CLICKHOUSE_CLIENT -q "SYSTEM STOP REPLICATION QUEUES t_r2"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE t_r1 FROM Disk('backups', '$BACKUP_HIGH') SETTINGS structure_only = 1 FORMAT Null"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE t_r2 FROM Disk('backups', '$BACKUP_LOW') SETTINGS structure_only = 1 FORMAT Null"

echo '--- both replicas have the version of the backup restored first ---'
$CLICKHOUSE_CLIENT -q "SELECT name, metadata_version FROM system.tables WHERE database = currentDatabase() AND name IN ('t_r1', 't_r2') ORDER BY name"

$CLICKHOUSE_CLIENT -q "SYSTEM START REPLICATION QUEUES t_r2"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA t_r2"
echo '--- a later ALTER goes through on both replicas ---'
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_r1 ADD COLUMN extra UInt8 SETTINGS alter_sync = 2"
$CLICKHOUSE_CLIENT -q "SELECT name, metadata_version FROM system.tables WHERE database = currentDatabase() AND name IN ('t_r1', 't_r2') ORDER BY name"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_r1 SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_r2 SYNC"
