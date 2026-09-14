#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree
# Regression: `RENAME TABLE` must be rejected for a `leader_election` table even when
# the table is an unloaded lazy proxy (`lazy_load_tables = 1`). The default `Atomic`
# database renames a table via `checkTableCanBeRenamed` + `renameInMemory`; before the
# fix, `StorageTableProxy` did not forward `checkTableCanBeRenamed` to the nested
# storage, so an unloaded table bypassed the `StorageMergeTree` guard.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LAZY_DB="${CLICKHOUSE_DATABASE}_lazy"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${LAZY_DB}"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${LAZY_DB} ENGINE = Atomic SETTINGS lazy_load_tables = 1"

# A per-database endpoint isolates this test's bucket prefix and metadata cache
# (see 04065_leader_election_basic.sh for the rationale).
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${LAZY_DB}.t (x UInt64)
    ENGINE = MergeTree ORDER BY x
    SETTINGS
        disk = disk(
            name = '04337_le_${CLICKHOUSE_DATABASE}',
            type = s3_plain_rewritable,
            endpoint = 'http://localhost:11111/test/04337_le_${CLICKHOUSE_DATABASE}/',
            access_key_id = clickhouse,
            secret_access_key = clickhouse),
        leader_election = true,
        leader_election_heartbeat_interval = 1, leader_election_session_timeout = 5
"

# After re-attach the table is an unloaded proxy.
$CLICKHOUSE_CLIENT -q "DETACH DATABASE ${LAZY_DB}"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE ${LAZY_DB}"
$CLICKHOUSE_CLIENT -q "SELECT name, engine FROM system.tables WHERE database = '${LAZY_DB}'"

# The rename must materialize the nested storage and hit the `leader_election` guard.
$CLICKHOUSE_CLIENT -q "RENAME TABLE ${LAZY_DB}.t TO ${LAZY_DB}.t_new" 2>&1 \
    | grep -o -m1 "SUPPORT_IS_DISABLED"

# The table keeps its old name.
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = '${LAZY_DB}'"

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${LAZY_DB}"
