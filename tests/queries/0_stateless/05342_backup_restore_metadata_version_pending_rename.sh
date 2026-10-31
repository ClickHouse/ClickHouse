#!/usr/bin/env bash
# Tags: no-replicated-database
# RESTORE raises the metadata version of an existing ReplicatedMergeTree table to the version saved in the backup, and
# the restored parts get that version. A replica lagging behind on a RENAME COLUMN does not have the structure the table
# has at that version, so the RESTORE must fail instead of loading parts which would lose the values of the column.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BACKUP_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}"

create_tables() {
    $CLICKHOUSE_CLIENT -q "CREATE TABLE t_r1 (id UInt64, x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r1') ORDER BY id"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE t_r2 (id UInt64, x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r2') ORDER BY id"
}

create_tables
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_r1 ADD COLUMN tmp UInt8 SETTINGS alter_sync = 2"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_r1 DROP COLUMN tmp SETTINGS alter_sync = 2"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_r1 SELECT number, number * 10 FROM numbers(3)"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA t_r2"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE t_r2 TO Disk('backups', '$BACKUP_NAME') FORMAT Null"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_r1 SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_r2 SYNC"

create_tables
$CLICKHOUSE_CLIENT -q "SYSTEM STOP REPLICATION QUEUES t_r2"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_r1 RENAME COLUMN x TO y SETTINGS alter_sync = 1"

echo '--- metadata versions of the replicas: r2 lags behind on the RENAME ---'
$CLICKHOUSE_CLIENT -q "SELECT name, metadata_version FROM system.tables WHERE database = currentDatabase() AND name IN ('t_r1', 't_r2') ORDER BY name"

echo '--- the backup has metadata version 2, which r2 cannot be marked with ---'
$CLICKHOUSE_CLIENT -q "RESTORE TABLE t_r2 FROM Disk('backups', '$BACKUP_NAME') FORMAT Null" 2>&1 | grep -c -m1 'INCOMPATIBLE_COLUMNS'

$CLICKHOUSE_CLIENT -q "SYSTEM START REPLICATION QUEUES t_r2"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA t_r2"
echo '--- no parts were restored ---'
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_r2"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_r1 SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_r2 SYNC"
