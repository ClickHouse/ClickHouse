#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Tag no-parallel: user-defined types live in a single process-wide namespace, and the backup of
# `system.user_defined_types` contains all of them.
# Tag no-fasttest: uses the `backups` disk.

# `BACKUP TABLE system.user_defined_types` saves the definitions of user-defined types, and `RESTORE` recreates them,
# registering every type after the types it is defined through.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `A` is defined through `Z`, so restoring the types in the order of their names would fail.
A="BackupA_${CLICKHOUSE_DATABASE}"
Z="BackupZ_${CLICKHOUSE_DATABASE}"
BACKUP="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

function show_types()
{
    $CLICKHOUSE_CLIENT --query "SELECT name, create_query FROM system.user_defined_types WHERE name IN ('${A}', '${Z}') ORDER BY name" \
        | sed "s/${CLICKHOUSE_DATABASE}/DB/g"
}

$CLICKHOUSE_CLIENT --query "DROP TYPE IF EXISTS ${A}"
$CLICKHOUSE_CLIENT --query "DROP TYPE IF EXISTS ${Z}"
$CLICKHOUSE_CLIENT --query "CREATE TYPE ${Z}(T) AS Array(T)"
$CLICKHOUSE_CLIENT --query "CREATE TYPE ${A} AS Tuple(UInt64, ${Z}(String))"

$CLICKHOUSE_CLIENT --query "BACKUP TABLE system.user_defined_types TO ${BACKUP}" | cut -f2

$CLICKHOUSE_CLIENT --query "DROP TYPE ${A}"
$CLICKHOUSE_CLIENT --query "DROP TYPE ${Z}"
echo "--- dropped"
show_types

$CLICKHOUSE_CLIENT --query "RESTORE TABLE system.user_defined_types FROM ${BACKUP}" | cut -f2
echo "--- restored"
show_types
$CLICKHOUSE_CLIENT --query "SELECT toTypeName(CAST((1, ['a']), '${A}'))"

# By default the types that already exist are kept.
$CLICKHOUSE_CLIENT --query "CREATE TYPE OR REPLACE ${Z}(T) AS Array(Nullable(T))"
$CLICKHOUSE_CLIENT --query "RESTORE TABLE system.user_defined_types FROM ${BACKUP}" | cut -f2
echo "--- kept"
show_types

# With `create_function = 'replace'` they are replaced.
$CLICKHOUSE_CLIENT --query "RESTORE TABLE system.user_defined_types FROM ${BACKUP} SETTINGS create_function = 'replace'" | cut -f2
echo "--- replaced"
show_types

$CLICKHOUSE_CLIENT --query "DROP TYPE ${A}"
$CLICKHOUSE_CLIENT --query "DROP TYPE ${Z}"
