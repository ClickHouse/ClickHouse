#!/usr/bin/env bash
# Tags: no-replicated-database
# The definition of a ReplicatedMergeTree table is written to a backup from ZooKeeper, so on a replica lagging
# behind on an ALTER it already has the altered structure. The metadata version saved next to it must match that
# definition, not the older version the lagging replica has in memory, even when no data is backed up.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BACKUP_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_r1 (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r1') ORDER BY id"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_r2 (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r2') ORDER BY id"

$CLICKHOUSE_CLIENT -q "SYSTEM STOP REPLICATION QUEUES t_r2"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_r1 ADD COLUMN c UInt8 SETTINGS alter_sync = 1"

echo '--- metadata versions of the replicas: r2 lags behind on the ALTER ---'
$CLICKHOUSE_CLIENT -q "SELECT name, metadata_version FROM system.tables WHERE database = currentDatabase() AND name IN ('t_r1', 't_r2') ORDER BY name"

$CLICKHOUSE_CLIENT -q "BACKUP TABLE t_r2 TO Disk('backups', '$BACKUP_NAME') SETTINGS structure_only = 1 FORMAT Null"

$CLICKHOUSE_CLIENT -q "SYSTEM START REPLICATION QUEUES t_r2"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_r1 SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_r2 SYNC"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE t_r2 FROM Disk('backups', '$BACKUP_NAME') SETTINGS structure_only = 1 FORMAT Null"

echo '--- the restored table has the column added by the ALTER r2 had not applied ---'
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_r2' ORDER BY name"
echo '--- and the metadata version that goes with it ---'
$CLICKHOUSE_CLIENT -q "SELECT metadata_version FROM system.tables WHERE database = currentDatabase() AND name = 't_r2'"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_r2 SYNC"
