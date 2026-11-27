#!/usr/bin/env bash
# Tags: no-replicated-database, no-ordinary-database
# Regression test for `RENAME DATABASE` losing view dependencies of a source
# table that has multiple dependent materialized views.
#
# Root cause: in `updateDatabaseName`, only the `getDependents` (MV-side) edges
# of each table were rewired. For a source table the relevant edges are the
# `getDependencies` (source-side) ones, which were never re-keyed under the
# new database name. After the rename, lookups by the new name returned an
# empty list of dependent views.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_OLD="${CLICKHOUSE_DATABASE}_04141_old"
DB_NEW="${CLICKHOUSE_DATABASE}_04141_new"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB_OLD} SYNC" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB_NEW} SYNC" 2>/dev/null ||:
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -nm -q "
    CREATE DATABASE ${DB_OLD} ENGINE = Atomic;
    CREATE TABLE ${DB_OLD}.src (c0 Int) ENGINE = Memory;
    CREATE TABLE ${DB_OLD}.target (c0 Int) ENGINE = Memory;
    CREATE MATERIALIZED VIEW ${DB_OLD}.mv1 TO ${DB_OLD}.target AS SELECT c0 FROM ${DB_OLD}.src;
    CREATE MATERIALIZED VIEW ${DB_OLD}.mv2 TO ${DB_OLD}.target AS SELECT c0 + 100 AS c0 FROM ${DB_OLD}.src;

    SELECT 'before', arraySort(dependencies_table) FROM system.tables WHERE database = '${DB_OLD}' AND name = 'src';

    RENAME DATABASE ${DB_OLD} TO ${DB_NEW};

    -- With the fix: both mv1 and mv2 are still listed as dependents of src.
    -- Without the fix: source-side view-dependency edges stay keyed under the
    -- old database name, so the lookup by the new name returns an empty list.
    SELECT 'after', arraySort(dependencies_table) FROM system.tables WHERE database = '${DB_NEW}' AND name = 'src';
"
