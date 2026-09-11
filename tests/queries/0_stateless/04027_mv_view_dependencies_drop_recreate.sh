#!/usr/bin/env bash
# Tags: no-replicated-database, no-fasttest
# Test that modifying a materialized view's query does not leak stale view dependency edges.
# Root cause: in `updateDependencies`, view dependency removal was inside
# `if (!new_view_dependencies.empty())`, so `ALTER TABLE mv MODIFY QUERY SELECT 1 c0`
# (with no view dependencies) would leak the stale edge. Combined with `BACKUP`/`RESTORE`
# this accumulated multiple dependents, violating `assert(tables_from.size() == 1)`.

# Suppress the expected `BackupMetadataFinder` warnings from the `RESTORE` step.
# The view's referenced tables are not in the backup (only the view itself is)
# and have been truncated from the database, so `RESTORE` emits benign warnings
# about the missing dependencies.
export CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_04027"
BACKUP_NAME="${CLICKHOUSE_DATABASE}_04027_backup"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}" 2>/dev/null ||:
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -nm -q "
    CREATE DATABASE ${DB} ENGINE = Memory;
    CREATE TABLE ${DB}.src1 (c0 Int) ENGINE = Memory;
    CREATE TABLE ${DB}.src2 (c0 Int) ENGINE = Memory;
    CREATE TABLE ${DB}.target (c0 Int) ENGINE = Memory;
    CREATE MATERIALIZED VIEW ${DB}.mv TO ${DB}.target AS SELECT c0 FROM ${DB}.src1;

    BACKUP VIEW ${DB}.mv TO Memory('${BACKUP_NAME}') SETTINGS id='${BACKUP_NAME}' FORMAT Null;

    -- Cleanly replaces: src1->mv becomes src2->mv.
    ALTER TABLE ${DB}.mv MODIFY QUERY SELECT c0 FROM ${DB}.src2;

    -- Without the fix: empty new_view_dependencies skipped removal, leaking the stale src2->mv edge.
    -- With the fix: src2 has no dependents.
    ALTER TABLE ${DB}.mv MODIFY QUERY SELECT 1 c0;
    SELECT 'after constant query', name, dependencies_table FROM system.tables WHERE database = '${DB}' AND name IN ('src1', 'src2') ORDER BY name;

    TRUNCATE DATABASE ${DB};

    RESTORE VIEW ${DB}.mv FROM Memory('${BACKUP_NAME}') SETTINGS id='${BACKUP_NAME}_restore' FORMAT Null;

    -- Without the fix this would also hit assert(tables_from.size() == 1) in updateDependencies (debug builds).
    ALTER TABLE ${DB}.mv MODIFY COMMENT '';

    SELECT 'OK';
"
