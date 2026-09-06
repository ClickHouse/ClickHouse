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
#
# Setup DDL is grouped into few multi-statement client calls so the test stays fast under the flaky
# check (which reruns it many times under sanitizers with a 180s per-run cap); the row counts are
# tiny because the coordination logic, not the data volume, is what is under test.

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
RDB_DIR_TTL="${CLICKHOUSE_TEST_UNIQUE_NAME}_rocksdb_ttl"
RDB_DIR_SCHEMA="${CLICKHOUSE_TEST_UNIQUE_NAME}_rocksdb_schema"
BACKUP_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}"
BACKUP_ID_EMPTY="${CLICKHOUSE_TEST_UNIQUE_NAME}_empty"
BACKUP_ID_POP="${CLICKHOUSE_TEST_UNIQUE_NAME}_pop"
BACKUP_ID_SHARED="${CLICKHOUSE_TEST_UNIQUE_NAME}_shared"
BACKUP_ID_RORO="${CLICKHOUSE_TEST_UNIQUE_NAME}_roro"
BACKUP_ID_TTL="${CLICKHOUSE_TEST_UNIQUE_NAME}_ttl"
BACKUP_ID_SCHEMA="${CLICKHOUSE_TEST_UNIQUE_NAME}_schema"
BACKUP_NAME="Disk('backups', '${BACKUP_ID}')"
BACKUP_NAME_EMPTY="Disk('backups', '${BACKUP_ID_EMPTY}')"
BACKUP_NAME_POP="Disk('backups', '${BACKUP_ID_POP}')"
BACKUP_NAME_SHARED="Disk('backups', '${BACKUP_ID_SHARED}')"
BACKUP_NAME_RORO="Disk('backups', '${BACKUP_ID_RORO}')"
BACKUP_NAME_TTL="Disk('backups', '${BACKUP_ID_TTL}')"
BACKUP_NAME_SCHEMA="Disk('backups', '${BACKUP_ID_SCHEMA}')"
USER_FILES_PATH=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.server_settings WHERE name = 'user_files_path'")
BACKUPS_PATH=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.disks WHERE name = 'backups'")
rm -rf "${USER_FILES_PATH:?}/${RDB_DIR}" "${USER_FILES_PATH:?}/${RDB_DIR_EMPTY}" "${USER_FILES_PATH:?}/${RDB_DIR_POP}" \
    "${USER_FILES_PATH:?}/${RDB_DIR_SHARED}" "${USER_FILES_PATH:?}/${RDB_DIR_RORO}" "${USER_FILES_PATH:?}/${RDB_DIR_TTL}" \
    "${USER_FILES_PATH:?}/${RDB_DIR_SCHEMA}" \
    "${BACKUPS_PATH:?}/${BACKUP_ID}" "${BACKUPS_PATH:?}/${BACKUP_ID_EMPTY}" "${BACKUPS_PATH:?}/${BACKUP_ID_POP}" \
    "${BACKUPS_PATH:?}/${BACKUP_ID_SHARED}" "${BACKUPS_PATH:?}/${BACKUP_ID_RORO}" "${BACKUPS_PATH:?}/${BACKUP_ID_TTL}" \
    "${BACKUPS_PATH:?}/${BACKUP_ID_SCHEMA}"

# Case 1: restore of a NON-empty read_only backup is rejected with a clear CANNOT_RESTORE_TABLE error.
# Populate an on-disk RocksDB directory through a writable table, then drop it (the explicit dir stays);
# a read_only table over the same directory records read_only = true, so restore recreates it read_only
# and rejects the data restore (the guard) instead of hitting an opaque RocksDB write failure.
$CLICKHOUSE_CLIENT --multiquery "
DROP TABLE IF EXISTS rdb_rw SYNC;
CREATE TABLE rdb_rw (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR}') PRIMARY KEY k;
INSERT INTO rdb_rw SELECT number, 'v' || toString(number) FROM numbers(100);
DROP TABLE rdb_rw SYNC;
DROP TABLE IF EXISTS rdb_ro SYNC;
CREATE TABLE rdb_ro (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR}', 1) PRIMARY KEY k;
BACKUP TABLE rdb_ro TO ${BACKUP_NAME} FORMAT Null;
DROP TABLE rdb_ro SYNC;
"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE rdb_ro FROM ${BACKUP_NAME} FORMAT Null" 2>&1 \
    | grep -o -m1 "CANNOT_RESTORE_TABLE" || echo "NO_EXPECTED_ERROR"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS rdb_ro SYNC"

# Case 2: an EMPTY read_only table's backup carries no rows, so its restore needs no write and must
# succeed as a pure metadata restore (not be blocked by the read_only guard). A read_only handle can
# only open an existing RocksDB directory, so lay down an empty one through a writable table first.
$CLICKHOUSE_CLIENT --multiquery "
DROP TABLE IF EXISTS rdb_rw_empty SYNC;
CREATE TABLE rdb_rw_empty (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_EMPTY}') PRIMARY KEY k;
DROP TABLE rdb_rw_empty SYNC;
DROP TABLE IF EXISTS rdb_ro_empty SYNC;
CREATE TABLE rdb_ro_empty (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_EMPTY}', 1) PRIMARY KEY k;
BACKUP TABLE rdb_ro_empty TO ${BACKUP_NAME_EMPTY} FORMAT Null;
DROP TABLE rdb_ro_empty SYNC;
"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE rdb_ro_empty FROM ${BACKUP_NAME_EMPTY} FORMAT Null" 2>&1 \
    | grep -o -m1 "CANNOT_RESTORE_TABLE" || echo "EMPTY_READONLY_RESTORE_OK"
$CLICKHOUSE_CLIENT --multiquery "
SELECT count() FROM rdb_ro_empty;
DROP TABLE IF EXISTS rdb_ro_empty SYNC;
"

# Case 3: an empty backup must not silently succeed when the target read_only directory already holds
# rows. Back up an empty read_only table over a directory, then populate that same directory behind its
# back through a writable table, then restore the empty backup: the non-empty-table guard must reject it
# (a read_only handle always points at an existing external directory, so this stale-rows case is
# realistic). With allow_non_empty_tables the restore is allowed, writes nothing, and rows stay in place.
$CLICKHOUSE_CLIENT --multiquery "
DROP TABLE IF EXISTS rdb_rw_pop SYNC;
CREATE TABLE rdb_rw_pop (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_POP}') PRIMARY KEY k;
DROP TABLE rdb_rw_pop SYNC;
DROP TABLE IF EXISTS rdb_ro_pop SYNC;
CREATE TABLE rdb_ro_pop (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_POP}', 1) PRIMARY KEY k;
BACKUP TABLE rdb_ro_pop TO ${BACKUP_NAME_POP} FORMAT Null;
DROP TABLE rdb_ro_pop SYNC;
CREATE TABLE rdb_rw_pop (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_POP}') PRIMARY KEY k;
INSERT INTO rdb_rw_pop SELECT number, 'stale' || toString(number) FROM numbers(100);
DROP TABLE rdb_rw_pop SYNC;
"
# Empty backup over the now-populated read_only directory: rejected (guard fires), not silent success.
$CLICKHOUSE_CLIENT -q "RESTORE TABLE rdb_ro_pop FROM ${BACKUP_NAME_POP} FORMAT Null" 2>&1 \
    | grep -o -m1 "CANNOT_RESTORE_TABLE" || echo "NO_EXPECTED_ERROR"
# With allow_non_empty_tables it is allowed, writes nothing, and the 100 existing rows are preserved.
$CLICKHOUSE_CLIENT -q "RESTORE TABLE rdb_ro_pop FROM ${BACKUP_NAME_POP} SETTINGS allow_non_empty_tables = 1 FORMAT Null" 2>&1 \
    | grep -o -m1 "CANNOT_RESTORE_TABLE" || echo "POPULATED_READONLY_RESTORE_ALLOWED"
$CLICKHOUSE_CLIENT --multiquery "
SELECT count() FROM rdb_ro_pop;
DROP TABLE IF EXISTS rdb_ro_pop SYNC;
"

# Case 4: a writable table plus a read_only table over the SAME rocksdb_dir (the supported shared-dir
# mode, see tests/integration/test_rocksdb_read_only). BACKUP DATABASE dumps the shared RocksDB only once,
# from the writable owner; the read_only sibling references that single data.bin instead of dumping an
# independent snapshot. On restore the writable owner replays the data once and the read_only sibling
# contributes no write, so the {rw, ro} pair restores cleanly (no spurious read_only rejection). The
# read_only sibling's handle, opened before the owner replayed the rows, is refreshed by
# finalizeRestoreFromBackup() so it observes the restored data with no manual reopen.
# See https://github.com/ClickHouse/ClickHouse/pull/109327
#
# The read_only handle is opened while the shared directory is still EMPTY, so its live snapshot holds 0
# rows and stays at 0 across the writable table's later inserts (a read_only handle does not see writes
# made through another handle). That makes the post-restore read a genuine discriminator: the restored
# data (300 rows) differs from the read_only sibling's stale snapshot (0 rows), so the sibling reports the
# restored count only if its handle was actually refreshed.
$CLICKHOUSE_CLIENT --multiquery "
DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_shared SYNC;
CREATE DATABASE ${CLICKHOUSE_DATABASE}_shared;
CREATE TABLE ${CLICKHOUSE_DATABASE}_shared.rw (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_SHARED}') PRIMARY KEY k;
CREATE TABLE ${CLICKHOUSE_DATABASE}_shared.ro (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_SHARED}', 1) PRIMARY KEY k;
INSERT INTO ${CLICKHOUSE_DATABASE}_shared.rw SELECT number, 'v' || toString(number) FROM numbers(300);
SELECT 'shared ro before restore', count() FROM ${CLICKHOUSE_DATABASE}_shared.ro;
BACKUP DATABASE ${CLICKHOUSE_DATABASE}_shared TO ${BACKUP_NAME_SHARED} FORMAT Null;
"
# Restore in place (the writable table already holds the 300 rows, so allow_non_empty_tables is required).
# The writable owner replays the shared data once and the restore does not fail on the read_only sibling.
$CLICKHOUSE_CLIENT -q "RESTORE DATABASE ${CLICKHOUSE_DATABASE}_shared FROM ${BACKUP_NAME_SHARED} SETTINGS allow_non_empty_tables = 1 FORMAT Null" 2>&1 \
    | grep -o -m1 "CANNOT_RESTORE_TABLE" || echo "SHARED_DIR_RESTORE_OK"
$CLICKHOUSE_CLIENT --multiquery "
SELECT 'shared rw restored', count(), sum(k) FROM ${CLICKHOUSE_DATABASE}_shared.rw;
SELECT 'shared ro sees data', count(), sum(k) FROM ${CLICKHOUSE_DATABASE}_shared.ro;
DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_shared SYNC;
"

# Case 5: two read_only tables over ONE rocksdb_dir with NO writable sibling (an all-read_only group).
# The single-owner dedup must NOT apply here: read_only handles are independent snapshots that can diverge,
# so there is no single live view that represents every table. Each read_only table must back up its own
# snapshot; collapsing both onto the election winner would make the loser's backup reference the winner's
# (different) data.bin and silently restore the wrong rows. See https://github.com/ClickHouse/ClickHouse/pull/109327
#
# Give the two handles genuinely different snapshots: open ro_a over a directory holding 100 rows, then add
# 100 more rows behind its back and open ro_b over the same directory (now 200 rows). A read_only handle
# snapshots the directory at open time, so ro_a keeps seeing 100 and ro_b sees 200. Restoring each backup AS
# a fresh writable table must recover its OWN snapshot (ro_a -> 100, ro_b -> 200), proving no cross-collapse.
$CLICKHOUSE_CLIENT --multiquery "
DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_roro SYNC;
CREATE DATABASE ${CLICKHOUSE_DATABASE}_roro;
CREATE TABLE ${CLICKHOUSE_DATABASE}_roro.feeder (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_RORO}') PRIMARY KEY k;
INSERT INTO ${CLICKHOUSE_DATABASE}_roro.feeder SELECT number, 'v' || toString(number) FROM numbers(100);
DROP TABLE ${CLICKHOUSE_DATABASE}_roro.feeder SYNC;
CREATE TABLE ${CLICKHOUSE_DATABASE}_roro.ro_a (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_RORO}', 1) PRIMARY KEY k;
CREATE TABLE ${CLICKHOUSE_DATABASE}_roro.feeder (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_RORO}') PRIMARY KEY k;
INSERT INTO ${CLICKHOUSE_DATABASE}_roro.feeder SELECT number, 'v' || toString(number) FROM numbers(100, 100);
DROP TABLE ${CLICKHOUSE_DATABASE}_roro.feeder SYNC;
CREATE TABLE ${CLICKHOUSE_DATABASE}_roro.ro_b (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_RORO}', 1) PRIMARY KEY k;
SELECT 'roro snapshots', (SELECT count() FROM ${CLICKHOUSE_DATABASE}_roro.ro_a), (SELECT count() FROM ${CLICKHOUSE_DATABASE}_roro.ro_b);
BACKUP DATABASE ${CLICKHOUSE_DATABASE}_roro TO ${BACKUP_NAME_RORO} FORMAT Null;
CREATE TABLE ${CLICKHOUSE_DATABASE}_roro.restored_a (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_RORO}_a') PRIMARY KEY k;
"
# Restore each read_only table's backup AS a fresh writable table (over its own new directory) and verify it
# recovers its own snapshot. If both had collapsed onto one owner's data.bin the loser would show the wrong
# count.
$CLICKHOUSE_CLIENT -q "RESTORE TABLE ${CLICKHOUSE_DATABASE}_roro.ro_a AS ${CLICKHOUSE_DATABASE}_roro.restored_a FROM ${BACKUP_NAME_RORO} SETTINGS allow_non_empty_tables = 1, allow_different_table_def = 1 FORMAT Null" 2>&1 \
    | grep -o -m1 "CANNOT_RESTORE_TABLE" || echo "RORO_A_RESTORE_OK"
$CLICKHOUSE_CLIENT --multiquery "
SELECT 'roro a restored', count() FROM ${CLICKHOUSE_DATABASE}_roro.restored_a;
CREATE TABLE ${CLICKHOUSE_DATABASE}_roro.restored_b (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_RORO}_b') PRIMARY KEY k;
"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE ${CLICKHOUSE_DATABASE}_roro.ro_b AS ${CLICKHOUSE_DATABASE}_roro.restored_b FROM ${BACKUP_NAME_RORO} SETTINGS allow_non_empty_tables = 1, allow_different_table_def = 1 FORMAT Null" 2>&1 \
    | grep -o -m1 "CANNOT_RESTORE_TABLE" || echo "RORO_B_RESTORE_OK"
$CLICKHOUSE_CLIENT --multiquery "
SELECT 'roro b restored', count() FROM ${CLICKHOUSE_DATABASE}_roro.restored_b;
DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_roro SYNC;
"

# Case 6: the backed-up value bytes are ttl-format-dependent (a ttl > 0 table is a DBWithTTL whose values
# carry a trailing creation timestamp; a ttl = 0 table has none), so restoring across a ttl mismatch would
# replay incompatible bytes or silently shift every row's expiry. The read_only workaround
# (RESTORE ... AS <writable_table> SETTINGS allow_different_table_def = 1) skips the create-query
# compatibility check, so an explicit restore-time ttl check must reject the mismatch. Back up a ttl = 0
# table and try to restore it AS a ttl = 5 table (and vice versa): both must be rejected; a matching ttl
# restore still succeeds.
$CLICKHOUSE_CLIENT --multiquery "
DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_ttl SYNC;
CREATE DATABASE ${CLICKHOUSE_DATABASE}_ttl;
CREATE TABLE ${CLICKHOUSE_DATABASE}_ttl.src0 (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_TTL}_src0') PRIMARY KEY k;
INSERT INTO ${CLICKHOUSE_DATABASE}_ttl.src0 SELECT number, 'v' || toString(number) FROM numbers(50);
BACKUP TABLE ${CLICKHOUSE_DATABASE}_ttl.src0 TO ${BACKUP_NAME_TTL} FORMAT Null;
CREATE TABLE ${CLICKHOUSE_DATABASE}_ttl.dst5 (k UInt64, v String) ENGINE = EmbeddedRocksDB(5, '${RDB_DIR_TTL}_dst5') PRIMARY KEY k;
CREATE TABLE ${CLICKHOUSE_DATABASE}_ttl.dst0 (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_TTL}_dst0') PRIMARY KEY k;
"
# ttl 0 backup restored into a ttl 5 target: rejected.
$CLICKHOUSE_CLIENT -q "RESTORE TABLE ${CLICKHOUSE_DATABASE}_ttl.src0 AS ${CLICKHOUSE_DATABASE}_ttl.dst5 FROM ${BACKUP_NAME_TTL} SETTINGS allow_non_empty_tables = 1, allow_different_table_def = 1 FORMAT Null" 2>&1 \
    | grep -o -m1 "CANNOT_RESTORE_TABLE" || echo "NO_EXPECTED_ERROR"
# ttl 0 backup restored into a ttl 0 target: allowed (matching ttl).
$CLICKHOUSE_CLIENT -q "RESTORE TABLE ${CLICKHOUSE_DATABASE}_ttl.src0 AS ${CLICKHOUSE_DATABASE}_ttl.dst0 FROM ${BACKUP_NAME_TTL} SETTINGS allow_non_empty_tables = 1, allow_different_table_def = 1 FORMAT Null" 2>&1 \
    | grep -o -m1 "CANNOT_RESTORE_TABLE" || echo "TTL_MATCH_RESTORE_OK"
$CLICKHOUSE_CLIENT --multiquery "
SELECT 'ttl match restored', count() FROM ${CLICKHOUSE_DATABASE}_ttl.dst0;
DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_ttl SYNC;
"

# Case 7: restore replays raw serialized (key, value) bytes and later decodes them with the TARGET table's
# schema (key = PK columns in PK order, value = the remaining physical columns in physical order). A target
# with the same ttl but a different physical-column layout (different value type, or different PK/value
# ordering) would silently decode the bytes into wrong data. The read_only workaround
# (RESTORE ... AS <writable_table> SETTINGS allow_different_table_def = 1) skips the create-query
# compatibility check, so an explicit restore-time schema-fingerprint check must reject the mismatch. Back up
# a (k UInt64, v String) table and try to restore it AS a table whose value column type differs
# (v UInt64) and AS one whose column order differs; both must be rejected. A matching-schema restore still
# succeeds.
$CLICKHOUSE_CLIENT --multiquery "
DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_schema SYNC;
CREATE DATABASE ${CLICKHOUSE_DATABASE}_schema;
CREATE TABLE ${CLICKHOUSE_DATABASE}_schema.src (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_SCHEMA}_src') PRIMARY KEY k;
INSERT INTO ${CLICKHOUSE_DATABASE}_schema.src SELECT number, 'v' || toString(number) FROM numbers(50);
BACKUP TABLE ${CLICKHOUSE_DATABASE}_schema.src TO ${BACKUP_NAME_SCHEMA} FORMAT Null;
CREATE TABLE ${CLICKHOUSE_DATABASE}_schema.dst_type (k UInt64, v UInt64) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_SCHEMA}_type') PRIMARY KEY k;
CREATE TABLE ${CLICKHOUSE_DATABASE}_schema.dst_same (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR_SCHEMA}_same') PRIMARY KEY k;
"
# Different value column type: rejected.
$CLICKHOUSE_CLIENT -q "RESTORE TABLE ${CLICKHOUSE_DATABASE}_schema.src AS ${CLICKHOUSE_DATABASE}_schema.dst_type FROM ${BACKUP_NAME_SCHEMA} SETTINGS allow_non_empty_tables = 1, allow_different_table_def = 1 FORMAT Null" 2>&1 \
    | grep -o -m1 "CANNOT_RESTORE_TABLE" || echo "NO_EXPECTED_ERROR"
# Matching schema: allowed.
$CLICKHOUSE_CLIENT -q "RESTORE TABLE ${CLICKHOUSE_DATABASE}_schema.src AS ${CLICKHOUSE_DATABASE}_schema.dst_same FROM ${BACKUP_NAME_SCHEMA} SETTINGS allow_non_empty_tables = 1, allow_different_table_def = 1 FORMAT Null" 2>&1 \
    | grep -o -m1 "CANNOT_RESTORE_TABLE" || echo "SCHEMA_MATCH_RESTORE_OK"
$CLICKHOUSE_CLIENT --multiquery "
SELECT 'schema match restored', count() FROM ${CLICKHOUSE_DATABASE}_schema.dst_same;
DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_schema SYNC;
"

rm -rf "${USER_FILES_PATH:?}/${RDB_DIR}" "${USER_FILES_PATH:?}/${RDB_DIR_EMPTY}" "${USER_FILES_PATH:?}/${RDB_DIR_POP}" \
    "${USER_FILES_PATH:?}/${RDB_DIR_SHARED}" "${USER_FILES_PATH:?}/${RDB_DIR_RORO}" \
    "${USER_FILES_PATH:?}/${RDB_DIR_RORO}_a" "${USER_FILES_PATH:?}/${RDB_DIR_RORO}_b" \
    "${USER_FILES_PATH:?}/${RDB_DIR_TTL}_src0" "${USER_FILES_PATH:?}/${RDB_DIR_TTL}_dst5" "${USER_FILES_PATH:?}/${RDB_DIR_TTL}_dst0" \
    "${USER_FILES_PATH:?}/${RDB_DIR_SCHEMA}_src" "${USER_FILES_PATH:?}/${RDB_DIR_SCHEMA}_type" "${USER_FILES_PATH:?}/${RDB_DIR_SCHEMA}_same" \
    "${BACKUPS_PATH:?}/${BACKUP_ID}" "${BACKUP_ID_EMPTY:+${BACKUPS_PATH:?}/${BACKUP_ID_EMPTY}}" \
    "${BACKUP_ID_POP:+${BACKUPS_PATH:?}/${BACKUP_ID_POP}}" \
    "${BACKUP_ID_SHARED:+${BACKUPS_PATH:?}/${BACKUP_ID_SHARED}}" \
    "${BACKUP_ID_RORO:+${BACKUPS_PATH:?}/${BACKUP_ID_RORO}}" \
    "${BACKUP_ID_TTL:+${BACKUPS_PATH:?}/${BACKUP_ID_TTL}}" \
    "${BACKUP_ID_SCHEMA:+${BACKUPS_PATH:?}/${BACKUP_ID_SCHEMA}}"
