#!/usr/bin/env bash
# Tags: no-replicated-database
# Regression: `RENAME DATABASE` asks every table it holds whether the rename is allowed, via
# `IStorage::checkTableCanBeRenamedByDatabaseRename`. That hook rejects only a `leader_election`
# `MergeTree` table (`05099_leader_election_lazy_rename_database.sh` covers that rejection for a
# lazy table), so answering it must not force a lazily loaded table to be materialized and
# started. Otherwise a single `RENAME DATABASE` loads an entire `lazy_load_tables = 1` database,
# and an otherwise-allowed rename starts to depend on every unrelated table starting
# successfully — an unloaded `ReplicatedMergeTree` would have to reach Keeper, for instance.
#
# The table below has no `leader_election`, so it must still be an unloaded `TableProxy` after
# the rename.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LAZY_DB="${CLICKHOUSE_DATABASE}_lazy_plain_rename_db"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${LAZY_DB}"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${LAZY_DB}_new"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${LAZY_DB} ENGINE = Atomic SETTINGS lazy_load_tables = 1"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${LAZY_DB}.t (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${LAZY_DB}.t VALUES (42)"

# After re-attach the table is an unloaded proxy.
$CLICKHOUSE_CLIENT -q "DETACH DATABASE ${LAZY_DB}"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE ${LAZY_DB}"
$CLICKHOUSE_CLIENT -q "SELECT name, engine FROM system.tables WHERE database = '${LAZY_DB}'"

$CLICKHOUSE_CLIENT -q "RENAME DATABASE ${LAZY_DB} TO ${LAZY_DB}_new"

# The rename succeeded, and it did not materialize the table: still a proxy.
$CLICKHOUSE_CLIENT -q "SELECT name, engine FROM system.tables WHERE database = '${LAZY_DB}_new'"

# The data survived the rename, and reading it materializes the table as usual. Compare
# against `TableProxy` rather than a literal engine name so the check holds for any
# `MergeTree` flavour.
$CLICKHOUSE_CLIENT -q "SELECT x FROM ${LAZY_DB}_new.t"
$CLICKHOUSE_CLIENT -q "SELECT engine != 'TableProxy' FROM system.tables WHERE database = '${LAZY_DB}_new' AND name = 't'"

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${LAZY_DB}_new"
