#!/usr/bin/env bash
# Tags: no-fasttest, use-rocksdb
# Tag no-fasttest: rocksdb engine is not enabled in fasttest build (ENABLE_LIBRARIES=0)

# A read_only EmbeddedRocksDB handle is opened with OpenForReadOnly() / DBWithTTL::Open(..., read_only)
# and rejects writes, so restoring rows into a read_only table cannot work: restore of a non-empty
# backup must fail up front with a clear CANNOT_RESTORE_TABLE error instead of an opaque RocksDB write
# error. A backup of an empty read_only table carries no rows, so its restore needs no write and is
# allowed as a pure metadata restore. Even then the non-empty-table guard still applies, so an empty
# backup restored over an already-populated read_only directory is rejected (not silently accepted).
# See https://github.com/ClickHouse/ClickHouse/issues/109213

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# On-disk directory (relative to user_files) and backup name, both scoped to this test's unique name
# so parallel runs do not collide.
RDB_DIR="${CLICKHOUSE_TEST_UNIQUE_NAME}_rocksdb"
RDB_DIR_EMPTY="${CLICKHOUSE_TEST_UNIQUE_NAME}_rocksdb_empty"
RDB_DIR_POP="${CLICKHOUSE_TEST_UNIQUE_NAME}_rocksdb_pop"
RDB_DIR_SHARED="${CLICKHOUSE_TEST_UNIQUE_NAME}_rocksdb_shared"
RDB_DIR_RORO="${CLICKHOUSE_TEST_UNIQUE_NAME}_rocksdb_roro"
BACKUP_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}"
BACKUP_ID_EMPTY="${CLICKHOUSE_TEST_UNIQUE_NAME}_empty"
BACKUP_ID_POP="${CLICKHOUSE_TEST_UNIQUE_NAME}_pop"
BACKUP_ID_SHARED="${CLICKHOUSE_TEST_UNIQUE_NAME}_shared"
BACKUP_ID_RORO="${CLICKHOUSE_TEST_UNIQUE_NAME}_roro"
BACKUP_NAME="Disk('backups', '${BACKUP_ID}')"
BACKUP_NAME_EMPTY="Disk('backups', '${BACKUP_ID_EMPTY}')"
BACKUP_NAME_POP="Disk('backups', '${BACKUP_ID_POP}')"
BACKUP_NAME_SHARED="Disk('backups', '${BACKUP_ID_SHARED}')"
BACKUP_NAME_RORO="Disk('backups', '${BACKUP_ID_RORO}')"
USER_FILES_PATH=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.server_settings WHERE name = 'user_files_path'")
BACKUPS_PATH=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.disks WHERE name = 'backups'")
rm -rf "${USER_FILES_PATH:?}/${RDB_DIR}" "${USER_FILES_PATH:?}/${RDB_DIR_EMPTY}" "${USER_FILES_PATH:?}/${RDB_DIR_POP}" \
    "${USER_FILES_PATH:?}/${RDB_DIR_SHARED}" "${USER_FILES_PATH:?}/${RDB_DIR_RORO}" \
    "${BACKUPS_PATH:?}/${BACKUP_ID}" "${BACKUPS_PATH:?}/${BACKUP_ID_EMPTY}" "${BACKUPS_PATH:?}/${BACKUP_ID_POP}" \
    "${BACKUPS_PATH:?}/${BACKUP_ID_SHARED}" "${BACKUPS_PATH:?}/${BACKUP_ID_RORO}"

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

# An empty backup must not silently succeed when the target read_only directory already holds rows.
# Back up an empty read_only table over a directory, then populate that same directory, then restore
# the empty backup: the non-empty-table guard must reject it (a read_only handle always points at an
# existing external directory, so this stale-rows case is realistic). With allow_non_empty_tables the
# restore is allowed, writes nothing, and the existing rows stay in place.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS rdb_rw_pop SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE rdb_rw_pop (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_POP}') PRIMARY KEY k"
$CLICKHOUSE_CLIENT -q "DROP TABLE rdb_rw_pop SYNC"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS rdb_ro_pop SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE rdb_ro_pop (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_POP}', 1) PRIMARY KEY k"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE rdb_ro_pop TO ${BACKUP_NAME_POP} FORMAT Null"
$CLICKHOUSE_CLIENT -q "DROP TABLE rdb_ro_pop SYNC"

# Populate the directory behind the read_only table's back through a writable table over the same dir.
$CLICKHOUSE_CLIENT -q "CREATE TABLE rdb_rw_pop (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_POP}') PRIMARY KEY k"
$CLICKHOUSE_CLIENT -q "INSERT INTO rdb_rw_pop SELECT number, 'stale' || toString(number) FROM numbers(100)"
$CLICKHOUSE_CLIENT -q "DROP TABLE rdb_rw_pop SYNC"

# Empty backup over the now-populated read_only directory: rejected (guard fires), not silent success.
$CLICKHOUSE_CLIENT -q "RESTORE TABLE rdb_ro_pop FROM ${BACKUP_NAME_POP} FORMAT Null" 2>&1 \
    | grep -o -m1 "CANNOT_RESTORE_TABLE" || echo "NO_EXPECTED_ERROR"
# With allow_non_empty_tables it is allowed, writes nothing, and the 100 existing rows are preserved.
$CLICKHOUSE_CLIENT -q "RESTORE TABLE rdb_ro_pop FROM ${BACKUP_NAME_POP} SETTINGS allow_non_empty_tables = 1 FORMAT Null" 2>&1 \
    | grep -o -m1 "CANNOT_RESTORE_TABLE" || echo "POPULATED_READONLY_RESTORE_ALLOWED"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM rdb_ro_pop"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS rdb_ro_pop SYNC"

# A writable table plus a read_only table over the SAME rocksdb_dir (the supported shared-dir mode, see
# tests/integration/test_rocksdb_read_only). BACKUP DATABASE dumps the shared RocksDB only once, from the
# writable owner; the read_only sibling references that single data.bin instead of dumping an independent
# snapshot. On restore the writable owner replays the data once and the read_only sibling contributes no
# write, so the {rw, ro} pair restores cleanly (no spurious read_only rejection). The read_only sibling's
# handle, opened before the owner replayed the rows, is refreshed by finalizeRestoreFromBackup() so it
# observes the restored data with no manual reopen. See https://github.com/ClickHouse/ClickHouse/pull/109327
#
# The read_only handle is opened while the shared directory is still EMPTY, so its live snapshot holds 0
# rows and stays at 0 across the writable table's later inserts (a read_only handle does not see writes made
# through another handle). That makes the post-restore read a genuine discriminator: the restored data (300
# rows) differs from the read_only sibling's stale snapshot (0 rows), so the sibling reports the restored
# count only if its handle was actually refreshed.
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_shared SYNC"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${CLICKHOUSE_DATABASE}_shared"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}_shared.rw (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_SHARED}') PRIMARY KEY k"
# Open the read_only sibling over the still-empty directory: its live snapshot is 0 rows.
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}_shared.ro (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_SHARED}', 1) PRIMARY KEY k"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${CLICKHOUSE_DATABASE}_shared.rw SELECT number, 'v' || toString(number) FROM numbers(300)"
# The read_only handle does not observe the writable table's live writes: it still snapshots 0 rows.
$CLICKHOUSE_CLIENT -q "SELECT 'shared ro before restore', count() FROM ${CLICKHOUSE_DATABASE}_shared.ro"
$CLICKHOUSE_CLIENT -q "BACKUP DATABASE ${CLICKHOUSE_DATABASE}_shared TO ${BACKUP_NAME_SHARED} FORMAT Null"

# Restore in place (the writable table already holds the 300 rows, so allow_non_empty_tables is required).
# The writable owner replays the shared data once and the restore does not fail on the read_only sibling.
$CLICKHOUSE_CLIENT -q "RESTORE DATABASE ${CLICKHOUSE_DATABASE}_shared FROM ${BACKUP_NAME_SHARED} SETTINGS allow_non_empty_tables = 1 FORMAT Null" 2>&1 \
    | grep -o -m1 "CANNOT_RESTORE_TABLE" || echo "SHARED_DIR_RESTORE_OK"
$CLICKHOUSE_CLIENT -q "SELECT 'shared rw restored', count(), sum(k) FROM ${CLICKHOUSE_DATABASE}_shared.rw"
# The read_only sibling now observes the restored data with no manual reopen: finalizeRestoreFromBackup()
# refreshed its handle after the owner replayed the rows (without that refresh it would still report 0).
$CLICKHOUSE_CLIENT -q "SELECT 'shared ro sees data', count(), sum(k) FROM ${CLICKHOUSE_DATABASE}_shared.ro"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_shared SYNC"

# Two read_only tables over ONE rocksdb_dir with NO writable sibling (an all-read_only group). The
# single-owner dedup must NOT apply here: read_only handles are independent snapshots that can diverge, so
# there is no single live view that represents every table. Each read_only table must back up its own
# snapshot; collapsing both onto the election winner would make the loser's backup reference the winner's
# (different) data.bin and silently restore the wrong rows. See https://github.com/ClickHouse/ClickHouse/pull/109327
#
# Give the two handles genuinely different snapshots: open ro_a over a directory holding 100 rows, then add
# 100 more rows behind its back and open ro_b over the same directory (now 200 rows). A read_only handle
# snapshots the directory at open time, so ro_a keeps seeing 100 and ro_b sees 200. Restoring each backup AS
# a fresh writable table must recover its OWN snapshot (ro_a -> 100, ro_b -> 200), proving no cross-collapse.
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_roro SYNC"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${CLICKHOUSE_DATABASE}_roro"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}_roro.feeder (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_RORO}') PRIMARY KEY k"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${CLICKHOUSE_DATABASE}_roro.feeder SELECT number, 'v' || toString(number) FROM numbers(100)"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${CLICKHOUSE_DATABASE}_roro.feeder SYNC"
# ro_a snapshots the directory at 100 rows.
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}_roro.ro_a (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_RORO}', 1) PRIMARY KEY k"
# Add 100 more rows to the directory behind ro_a's back, then open ro_b over the now-200-row directory.
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}_roro.feeder (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_RORO}') PRIMARY KEY k"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${CLICKHOUSE_DATABASE}_roro.feeder SELECT number, 'v' || toString(number) FROM numbers(100, 100)"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${CLICKHOUSE_DATABASE}_roro.feeder SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}_roro.ro_b (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_RORO}', 1) PRIMARY KEY k"
# The two read_only handles hold different snapshots: ro_a sees 100 rows, ro_b sees 200.
$CLICKHOUSE_CLIENT -q "SELECT 'roro snapshots', (SELECT count() FROM ${CLICKHOUSE_DATABASE}_roro.ro_a), (SELECT count() FROM ${CLICKHOUSE_DATABASE}_roro.ro_b)"
$CLICKHOUSE_CLIENT -q "BACKUP DATABASE ${CLICKHOUSE_DATABASE}_roro TO ${BACKUP_NAME_RORO} FORMAT Null"
# Restore each read_only table's backup AS a fresh writable table (over its own new directory) and verify it
# recovers its own snapshot. If both had collapsed onto one owner's data.bin the loser would show the wrong
# count.
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}_roro.restored_a (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_RORO}_a') PRIMARY KEY k"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE ${CLICKHOUSE_DATABASE}_roro.ro_a AS ${CLICKHOUSE_DATABASE}_roro.restored_a FROM ${BACKUP_NAME_RORO} SETTINGS allow_non_empty_tables = 1, allow_different_table_def = 1 FORMAT Null" 2>&1 \
    | grep -o -m1 "CANNOT_RESTORE_TABLE" || echo "RORO_A_RESTORE_OK"
$CLICKHOUSE_CLIENT -q "SELECT 'roro a restored', count() FROM ${CLICKHOUSE_DATABASE}_roro.restored_a"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}_roro.restored_b (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_RORO}_b') PRIMARY KEY k"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE ${CLICKHOUSE_DATABASE}_roro.ro_b AS ${CLICKHOUSE_DATABASE}_roro.restored_b FROM ${BACKUP_NAME_RORO} SETTINGS allow_non_empty_tables = 1, allow_different_table_def = 1 FORMAT Null" 2>&1 \
    | grep -o -m1 "CANNOT_RESTORE_TABLE" || echo "RORO_B_RESTORE_OK"
$CLICKHOUSE_CLIENT -q "SELECT 'roro b restored', count() FROM ${CLICKHOUSE_DATABASE}_roro.restored_b"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_roro SYNC"

rm -rf "${USER_FILES_PATH:?}/${RDB_DIR}" "${USER_FILES_PATH:?}/${RDB_DIR_EMPTY}" "${USER_FILES_PATH:?}/${RDB_DIR_POP}" \
    "${USER_FILES_PATH:?}/${RDB_DIR_SHARED}" "${USER_FILES_PATH:?}/${RDB_DIR_RORO}" \
    "${USER_FILES_PATH:?}/${RDB_DIR_RORO}_a" "${USER_FILES_PATH:?}/${RDB_DIR_RORO}_b" \
    "${BACKUPS_PATH:?}/${BACKUP_ID}" "${BACKUP_ID_EMPTY:+${BACKUPS_PATH:?}/${BACKUP_ID_EMPTY}}" \
    "${BACKUP_ID_POP:+${BACKUPS_PATH:?}/${BACKUP_ID_POP}}" \
    "${BACKUP_ID_SHARED:+${BACKUPS_PATH:?}/${BACKUP_ID_SHARED}}" \
    "${BACKUP_ID_RORO:+${BACKUPS_PATH:?}/${BACKUP_ID_RORO}}"
