#!/usr/bin/env bash
# Tags: no-fasttest, use-rocksdb

# A read_only EmbeddedRocksDB table whose data directory is emptied by TRUNCATE
# can no longer be reopened: initDB() calls OpenForReadOnly() on the now empty
# directory and throws, leaving rocksdb_ptr == nullptr. A subsequent TRUNCATE or
# DROP must not dereference the null handle (used to crash the server).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A directory relative to user_files; EmbeddedRocksDB resolves it itself.
RDB_DIR="04330_rocksdb_${CLICKHOUSE_DATABASE}"
USER_FILES_PATH=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.server_settings WHERE name = 'user_files_path'")

rm -rf "${USER_FILES_PATH:?}/${RDB_DIR}"

# Populate an on-disk RocksDB directory through a writable table, then drop it.
# DROP closes the writable handle but leaves the explicit rocksdb_dir on disk,
# so the files stay available for the read_only table below.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS rdb_rw SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE rdb_rw (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR}') PRIMARY KEY k"
$CLICKHOUSE_CLIENT -q "INSERT INTO rdb_rw VALUES (1, 'a')"
$CLICKHOUSE_CLIENT -q "DROP TABLE rdb_rw SYNC"

# Open the same directory read_only. TRUNCATE empties the directory and then
# fails to reopen it read_only, so rocksdb_ptr becomes null (expected error).
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS rdb_ro SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE rdb_ro (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR}', 1) PRIMARY KEY k"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE rdb_ro" >/dev/null 2>&1 ||:
# Second TRUNCATE and the synchronous DROP must not crash on the null handle.
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE rdb_ro" >/dev/null 2>&1 ||:
$CLICKHOUSE_CLIENT -q "DROP TABLE rdb_ro SYNC"

# Server is still alive.
$CLICKHOUSE_CLIENT -q "SELECT 'ok'"

rm -rf "${USER_FILES_PATH:?}/${RDB_DIR}"
