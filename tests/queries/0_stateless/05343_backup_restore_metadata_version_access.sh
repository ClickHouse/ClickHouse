#!/usr/bin/env bash
# Tags: no-replicated-database
# RESTORE into an existing ReplicatedMergeTree table can raise its metadata version on all its replicas, so it requires
# `INSERT` on the table even when no data is restored, and not only `SHOW TABLES`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BACKUP_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}"
user="user_05343_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $user"
$CLICKHOUSE_CLIENT -q "CREATE TABLE src (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/src', 'r1') ORDER BY id"
$CLICKHOUSE_CLIENT -q "ALTER TABLE src ADD COLUMN x UInt8"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE src TO Disk('backups', '$BACKUP_NAME') FORMAT Null"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t (id UInt64, x UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r1') ORDER BY id"
$CLICKHOUSE_CLIENT -q "CREATE USER $user"
$CLICKHOUSE_CLIENT -q "GRANT SHOW TABLES ON ${CLICKHOUSE_DATABASE}.t TO $user"
$CLICKHOUSE_CLIENT -q "GRANT READ ON DISK TO $user"

restore_query="RESTORE TABLE src AS t FROM Disk('backups', '$BACKUP_NAME') SETTINGS create_table = 'must-exist', structure_only = 1, allow_different_table_def = 1 FORMAT Null"

echo '--- without INSERT: denied, and the metadata version is unchanged ---'
$CLICKHOUSE_CLIENT --user "$user" -q "$restore_query" 2>&1 | grep -c -m1 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT -q "SELECT metadata_version FROM system.tables WHERE database = currentDatabase() AND name = 't'"

echo '--- with INSERT: allowed ---'
$CLICKHOUSE_CLIENT -q "GRANT INSERT ON ${CLICKHOUSE_DATABASE}.t TO $user"
$CLICKHOUSE_CLIENT --user "$user" -q "$restore_query"
$CLICKHOUSE_CLIENT -q "SELECT metadata_version FROM system.tables WHERE database = currentDatabase() AND name = 't'"

$CLICKHOUSE_CLIENT -q "DROP USER $user"
$CLICKHOUSE_CLIENT -q "DROP TABLE src SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE t SYNC"
