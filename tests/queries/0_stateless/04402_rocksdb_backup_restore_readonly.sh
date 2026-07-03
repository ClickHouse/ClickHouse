#!/usr/bin/env bash
# Tags: no-fasttest, use-rocksdb
# Tag no-fasttest: rocksdb engine is not enabled in fasttest build (ENABLE_LIBRARIES=0)

# A read_only EmbeddedRocksDB handle is opened with OpenForReadOnly() / DBWithTTL::Open(..., read_only)
# and rejects writes, so restoring rows into a read_only table cannot work: restore of a non-empty
# backup must fail up front with a clear CANNOT_RESTORE_TABLE error instead of an opaque RocksDB write
# error. A backup of an empty read_only table carries no rows, so its restore needs no write and is
# allowed as a pure metadata restore.
# See https://github.com/ClickHouse/ClickHouse/issues/109213

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# On-disk directory (relative to user_files) and backup name, both scoped to this test's unique name
# so parallel runs do not collide.
RDB_DIR="${CLICKHOUSE_TEST_UNIQUE_NAME}_rocksdb"
RDB_DIR_EMPTY="${CLICKHOUSE_TEST_UNIQUE_NAME}_rocksdb_empty"
BACKUP_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}"
BACKUP_ID_EMPTY="${CLICKHOUSE_TEST_UNIQUE_NAME}_empty"
BACKUP_NAME="Disk('backups', '${BACKUP_ID}')"
BACKUP_NAME_EMPTY="Disk('backups', '${BACKUP_ID_EMPTY}')"
USER_FILES_PATH=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.server_settings WHERE name = 'user_files_path'")
BACKUPS_PATH=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.disks WHERE name = 'backups'")
rm -rf "${USER_FILES_PATH:?}/${RDB_DIR}" "${USER_FILES_PATH:?}/${RDB_DIR_EMPTY}" \
    "${BACKUPS_PATH:?}/${BACKUP_ID}" "${BACKUPS_PATH:?}/${BACKUP_ID_EMPTY}"

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

# An EMPTY read_only table: its backup carries no rows, so restore needs no write and must succeed as
# a pure metadata restore (not be blocked by the read_only guard). A read_only handle can only open an
# existing RocksDB directory, so lay down an empty one through a writable table first, then drop it.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS rdb_rw_empty SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE rdb_rw_empty (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_EMPTY}') PRIMARY KEY k"
$CLICKHOUSE_CLIENT -q "DROP TABLE rdb_rw_empty SYNC"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS rdb_ro_empty SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE rdb_ro_empty (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_EMPTY}', 1) PRIMARY KEY k"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE rdb_ro_empty TO ${BACKUP_NAME_EMPTY} FORMAT Null"
$CLICKHOUSE_CLIENT -q "DROP TABLE rdb_ro_empty SYNC"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE rdb_ro_empty FROM ${BACKUP_NAME_EMPTY} FORMAT Null" 2>&1 \
    | grep -o -m1 "CANNOT_RESTORE_TABLE" || echo "EMPTY_READONLY_RESTORE_OK"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM rdb_ro_empty"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS rdb_ro_empty SYNC"
rm -rf "${USER_FILES_PATH:?}/${RDB_DIR}" "${USER_FILES_PATH:?}/${RDB_DIR_EMPTY}" \
    "${BACKUPS_PATH:?}/${BACKUP_ID}" "${BACKUP_ID_EMPTY:+${BACKUPS_PATH:?}/${BACKUP_ID_EMPTY}}"
