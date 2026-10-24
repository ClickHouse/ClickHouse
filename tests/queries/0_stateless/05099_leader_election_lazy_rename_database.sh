#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree
# Regression: `RENAME DATABASE` must be rejected for a `leader_election` table even when the table
# is an unloaded lazy proxy (`lazy_load_tables = 1`). `DatabaseAtomic::renameDatabase` enforces the
# guard by calling `IStorage::checkTableCanBeRenamedByDatabaseRename` on every table it holds, and
# for an unloaded table that is a `StorageTableProxy`, whose default implementation is a no-op. The
# proxy must therefore materialize the nested storage and forward the call — otherwise a
# database-level rename silently moves the metadata of a table whose shared lease path was captured
# at startup. `04339_leader_election_rename_database.sh` covers the eagerly loaded table; this is
# the lazy sibling of it, the same way `04337_leader_election_lazy_rename.sh` is the lazy sibling of
# the `RENAME TABLE` guard.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LAZY_DB="${CLICKHOUSE_DATABASE}_lazy_rename_db"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${LAZY_DB}"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${LAZY_DB} ENGINE = Atomic SETTINGS lazy_load_tables = 1"

# A per-database endpoint isolates this test's bucket prefix and metadata cache
# (see 04065_leader_election_basic.sh for the rationale).
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${LAZY_DB}.t (x UInt64)
    ENGINE = MergeTree ORDER BY x
    SETTINGS
        disk = disk(
            name = '05099_le_${CLICKHOUSE_DATABASE}',
            type = s3_plain_rewritable,
            endpoint = 'http://localhost:11111/test/05099_le_${CLICKHOUSE_DATABASE}/',
            access_key_id = clickhouse,
            secret_access_key = clickhouse),
        leader_election = true,
        leader_election_heartbeat_interval = 1, leader_election_session_timeout = 5
"

# After re-attach the table is an unloaded proxy.
$CLICKHOUSE_CLIENT -q "DETACH DATABASE ${LAZY_DB}"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE ${LAZY_DB}"
$CLICKHOUSE_CLIENT -q "SELECT name, engine FROM system.tables WHERE database = '${LAZY_DB}'"

# The database-level rename must materialize the nested storage and hit the `leader_election` guard.
$CLICKHOUSE_CLIENT -q "RENAME DATABASE ${LAZY_DB} TO ${LAZY_DB}_new" 2>&1 \
    | grep -o -m1 "SUPPORT_IS_DISABLED"

# The database keeps its old name (1), the renamed-to name does not exist (0),
# and the table is intact.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.databases WHERE name = '${LAZY_DB}'"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.databases WHERE name = '${LAZY_DB}_new'"
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = '${LAZY_DB}'"

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${LAZY_DB}"
