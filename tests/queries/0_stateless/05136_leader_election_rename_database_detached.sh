#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree
# Regression: `RENAME DATABASE` must also be rejected when the only `leader_election` table of the
# database is detached. `DETACH TABLE` moves the table out of the database's `tables` map into
# `snapshot_detached_tables`, so the guard that asks every attached storage
# (`IStorage::checkTableCanBeRenamedByDatabaseRename`) used to see nothing to reject, while the
# rename still moved the detached table's metadata along with the database — `DETACH TABLE`,
# `RENAME DATABASE`, `ATTACH TABLE` brought the very same shared-storage table back under a new
# database name while the peers kept tracking the old one. Both flavours of detach are covered.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_le_rename_db_detached"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB}"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${DB} ENGINE = Atomic"

# A per-database endpoint isolates this test's bucket prefix and metadata cache
# (see 04065_leader_election_basic.sh for the rationale).
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${DB}.t (x UInt64)
    ENGINE = MergeTree ORDER BY x
    SETTINGS
        disk = disk(
            name = '05136_le_${CLICKHOUSE_DATABASE}',
            type = s3_plain_rewritable,
            endpoint = 'http://localhost:11111/test/05136_le_${CLICKHOUSE_DATABASE}/',
            access_key_id = clickhouse,
            secret_access_key = clickhouse),
        leader_election = true,
        leader_election_heartbeat_interval = 1, leader_election_session_timeout = 5
"

# An ordinary detach leaves the table's metadata in the database, so the rename must be rejected.
$CLICKHOUSE_CLIENT -q "DETACH TABLE ${DB}.t"
$CLICKHOUSE_CLIENT -q "RENAME DATABASE ${DB} TO ${DB}_new" 2>&1 \
    | grep -o -m1 "SUPPORT_IS_DISABLED"

# A permanent detach is the same story: `ATTACH TABLE` brings the table back after a restart.
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${DB}.t"
$CLICKHOUSE_CLIENT -q "DETACH TABLE ${DB}.t PERMANENTLY"
$CLICKHOUSE_CLIENT -q "RENAME DATABASE ${DB} TO ${DB}_new" 2>&1 \
    | grep -o -m1 "SUPPORT_IS_DISABLED"

# The database keeps its old name (1), the renamed-to name does not exist (0),
# and the table is still attachable and intact.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.databases WHERE name = '${DB}'"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.databases WHERE name = '${DB}_new'"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${DB}.t"
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = '${DB}'"

# A database whose detached table cannot carry the guard is renamed as before.
$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.t SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.ordinary (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "DETACH TABLE ${DB}.ordinary"
$CLICKHOUSE_CLIENT -q "RENAME DATABASE ${DB} TO ${DB}_new"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.databases WHERE name = '${DB}_new'"

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${DB}_new"
