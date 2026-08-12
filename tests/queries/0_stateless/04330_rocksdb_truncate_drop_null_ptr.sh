#!/usr/bin/env bash
# Tags: no-fasttest, use-rocksdb

# A read_only EmbeddedRocksDB table whose data directory is emptied by TRUNCATE
# can no longer be reopened: initDB() calls OpenForReadOnly() on the now empty
# directory and throws, leaving rocksdb_ptr == nullptr. A subsequent TRUNCATE or
# DROP must not dereference the null handle (used to crash the server).
#
# Run via clickhouse-local: the poisoned read_only table lives only for the
# lifetime of this one-shot process, so it never persists into a server's
# metadata. A persistent read_only table over a relative user_files dir would
# brick server start-up if a restart reattached it after TRUNCATE wiped the dir.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Unique data dir so parallel runs never collide.
RDB_DIR="${CLICKHOUSE_TMP}/04330_rocksdb_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${RDB_DIR:?}"

# --ignore-error keeps the batch going past the expected reopen failure of the
# first TRUNCATE, so the second TRUNCATE and the DROP run against the now-null
# handle. The "SELECT count()" before the first TRUNCATE must print 1: it proves
# the read_only table actually opened over the data rdb_rw persisted (i.e. the
# EmbeddedRocksDB path was reached) rather than --ignore-error having silently
# swallowed a broken setup. With the fix the process then prints "ok"; without
# it the process crashes on the null handle.
${CLICKHOUSE_LOCAL} --ignore-error --multiquery "
CREATE TABLE rdb_rw (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR}') PRIMARY KEY k;
INSERT INTO rdb_rw VALUES (1, 'a');
DROP TABLE rdb_rw SYNC;
CREATE TABLE rdb_ro (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '${RDB_DIR}', 1) PRIMARY KEY k;
SELECT count() FROM rdb_ro;
TRUNCATE TABLE rdb_ro;
TRUNCATE TABLE rdb_ro;
DROP TABLE rdb_ro SYNC;
SELECT 'ok';
" 2>/dev/null

rm -rf "${RDB_DIR:?}"
