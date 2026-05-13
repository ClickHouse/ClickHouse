#!/usr/bin/env bash
# Tags: no-fasttest
# Test that RESTORE does not crash when the dependency graph contains cyclic dependencies.
# See https://github.com/ClickHouse/ClickHouse/issues/103746

# Suppress the expected BackupMetadataFinder warning about missing dependencies
# caused by the CTE name colliding with a real table name.
export CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="db_04141_${CLICKHOUSE_DATABASE}"
BACKUP_NAME="backup_04141_${CLICKHOUSE_DATABASE}"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB} SYNC" 2>/dev/null ||:
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -nm -q "
    CREATE DATABASE ${DB};

    CREATE TABLE ${DB}.src (id Int32) ENGINE = MergeTree() ORDER BY id;

    -- mv1 has a CTE named mv2 whose name collides with the real table mv2,
    -- creating a false cycle in the dependency graph: mv1 -> mv2 (CTE) AND mv2 -> mv1 (real).
    CREATE MATERIALIZED VIEW ${DB}.mv1 (id Int32) ENGINE = MergeTree() ORDER BY id
        AS WITH mv2 AS (SELECT id FROM ${DB}.src) SELECT * FROM mv2;

    CREATE MATERIALIZED VIEW ${DB}.mv2 (id Int32) ENGINE = MergeTree() ORDER BY id
        AS SELECT id FROM ${DB}.mv1;

    BACKUP DATABASE ${DB} TO Memory('${BACKUP_NAME}') FORMAT Null;

    DROP DATABASE ${DB} SYNC;

    -- Before the fix this would abort the server due to SIZE_MAX overflow in
    -- TablesDependencyGraph::getTablesSplitByDependencyLevel.
    RESTORE DATABASE ${DB} FROM Memory('${BACKUP_NAME}') FORMAT Null;

    SELECT 'server alive';
"
