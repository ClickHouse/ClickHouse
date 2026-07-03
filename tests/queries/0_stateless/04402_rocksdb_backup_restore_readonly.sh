#!/usr/bin/env bash
# Tags: no-fasttest, use-rocksdb
# Tag no-fasttest: rocksdb engine is not enabled in fasttest build (ENABLE_LIBRARIES=0)

# A read_only EmbeddedRocksDB handle is opened with OpenForReadOnly() / DBWithTTL::Open(..., read_only)
# and rejects writes, so restoring data into a read_only table cannot work. Restore must fail up front
# with a clear CANNOT_RESTORE_TABLE error instead of an opaque RocksDB write error.
# See https://github.com/ClickHouse/ClickHouse/issues/109213

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# On-disk directory (relative to user_files) and backup name, both scoped to this test's unique name
# so parallel runs do not collide.
RDB_DIR="${CLICKHOUSE_TEST_UNIQUE_NAME}_rocksdb"
BACKUP_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}"
BACKUP_NAME="Disk('backups', '${BACKUP_ID}')"
USER_FILES_PATH=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.server_settings WHERE name = 'user_files_path'")
BACKUPS_PATH=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.disks WHERE name = 'backups'")
rm -rf "${USER_FILES_PATH:?}/${RDB_DIR}" "${BACKUPS_PATH:?}/${BACKUP_ID}"

# Populate an on-disk RocksDB directory through a writable table, then drop it (the explicit dir stays).
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS rdb_rw SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE rdb_rw (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR}') PRIMARY KEY k"
$CLICKHOUSE_CLIENT -q "INSERT INTO rdb_rw SELECT number, 'v' || toString(number) FROM numbers(100)"
$CLICKHOUSE_CLIENT -q "DROP TABLE rdb_rw SYNC"

# A read_only table over the same directory. Its definition records read_only = true, so the backup
# metadata does too, and restore recreates it as read_only.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS rdb_ro SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE rdb_ro (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR}', 1) PRIMARY KEY k"

# Backup succeeds (it only reads).
$CLICKHOUSE_CLIENT -q "BACKUP TABLE rdb_ro TO ${BACKUP_NAME} FORMAT Null"
$CLICKHOUSE_CLIENT -q "DROP TABLE rdb_ro SYNC"

# Restore recreates the read_only table from the backup metadata and then rejects the data restore
# with a clear CANNOT_RESTORE_TABLE error (the guard) instead of an opaque RocksDB write failure.
$CLICKHOUSE_CLIENT -q "RESTORE TABLE rdb_ro FROM ${BACKUP_NAME} FORMAT Null" 2>&1 \
    | grep -o -m1 "CANNOT_RESTORE_TABLE" || echo "NO_EXPECTED_ERROR"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS rdb_ro SYNC"
rm -rf "${USER_FILES_PATH:?}/${RDB_DIR}" "${BACKUPS_PATH:?}/${BACKUP_ID}"
