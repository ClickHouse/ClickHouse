#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree
# Regression: `RENAME DATABASE` must be rejected for a `leader_election` table — the shared
# lease path is captured at startup and there is no protocol to broadcast a new path to
# followers. This is enforced via `IStorage::checkTableCanBeRenamedByDatabaseRename`, a
# database-rename-specific guard. It must NOT be implemented by reusing `checkTableCanBeRenamed`
# in `DatabaseAtomic::renameDatabase`, which would also break `RENAME DATABASE` for ordinary
# tables (e.g. the Ordinary-to-Atomic startup conversion, which ends with `RENAME DATABASE`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_le_rename_db"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB}"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${DB} ENGINE = Atomic"

# A per-database endpoint isolates this test's bucket prefix and metadata cache
# (see 04065_leader_election_basic.sh for the rationale).
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${DB}.t (x UInt64)
    ENGINE = MergeTree ORDER BY x
    SETTINGS
        disk = disk(
            name = '04339_le_${CLICKHOUSE_DATABASE}',
            type = s3_plain_rewritable,
            endpoint = 'http://localhost:11111/test/04339_le_${CLICKHOUSE_DATABASE}/',
            access_key_id = clickhouse,
            secret_access_key = clickhouse),
        leader_election = true,
        leader_election_heartbeat_interval = 1, leader_election_session_timeout = 5
"

# The database-level rename must hit the `leader_election` guard.
$CLICKHOUSE_CLIENT -q "RENAME DATABASE ${DB} TO ${DB}_new" 2>&1 \
    | grep -o -m1 "SUPPORT_IS_DISABLED"

# The database keeps its old name (1), the renamed-to name does not exist (0),
# and the table is intact.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.databases WHERE name = '${DB}'"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.databases WHERE name = '${DB}_new'"
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = '${DB}'"

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${DB}"
