#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

caller_database="execute_as_caller_${CLICKHOUSE_DATABASE}"
target_database="execute_as_target_${CLICKHOUSE_DATABASE}"
caller="execute_as_caller_${CLICKHOUSE_DATABASE}"
target="execute_as_target_${CLICKHOUSE_DATABASE}"

# The statement wrapped in `EXECUTE AS` belongs to the caller's query, so it keeps the current
# database of the caller's session instead of being re-scoped to the target user's default
# database. Both databases hold a table named `t`, so the unqualified name tells them apart:
# `caller` is the correct answer, `target` would mean the scope was switched.
#
# The connection carries an explicit `--database`, which takes precedence over the caller's
# `DEFAULT DATABASE`, so the current database of the session has to be set there as well.
CLICKHOUSE_CLIENT_CALLER=${CLICKHOUSE_CLIENT/--database=$CLICKHOUSE_DATABASE/--database=$caller_database}
CLICKHOUSE_CLIENT_NO_DATABASE=${CLICKHOUSE_CLIENT/--database=$CLICKHOUSE_DATABASE/}

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${caller}, ${target}"
    $CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${caller_database}"
    $CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${target_database}"
}

cleanup

$CLICKHOUSE_CLIENT --query "CREATE DATABASE ${caller_database}"
$CLICKHOUSE_CLIENT --query "CREATE DATABASE ${target_database}"
$CLICKHOUSE_CLIENT --query "CREATE TABLE ${caller_database}.t (value String) ENGINE = Memory"
$CLICKHOUSE_CLIENT --query "CREATE TABLE ${target_database}.t (value String) ENGINE = Memory"
$CLICKHOUSE_CLIENT --query "INSERT INTO ${caller_database}.t VALUES ('caller')"
$CLICKHOUSE_CLIENT --query "INSERT INTO ${target_database}.t VALUES ('target')"

$CLICKHOUSE_CLIENT --query "CREATE USER ${caller} IDENTIFIED WITH no_password DEFAULT DATABASE ${caller_database}"
$CLICKHOUSE_CLIENT --query "CREATE USER ${target} IDENTIFIED WITH no_password DEFAULT DATABASE ${target_database}"
$CLICKHOUSE_CLIENT --query "GRANT IMPERSONATE ON ${target} TO ${caller}"
# The target is allowed to read both tables, so a lost current database shows up as the wrong
# value rather than as an access error.
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON ${caller_database}.t TO ${target}"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON ${target_database}.t TO ${target}"

echo "-- subquery form"
$CLICKHOUSE_CLIENT_CALLER --user "${caller}" --query "EXECUTE AS ${target} SELECT value FROM t"

echo "-- session form"
$CLICKHOUSE_CLIENT_CALLER --user "${caller}" --query "EXECUTE AS ${target}; SELECT value FROM t;"

# An empty current database is a scope as well. It must not be replaced with the target user's
# default database, which would make the following unqualified name unexpectedly resolve to `target_database`.
echo "-- empty database subquery form"
$CLICKHOUSE_CLIENT_NO_DATABASE --user "${caller}" --query "EXECUTE AS ${target} SELECT value FROM t" 2>&1 | grep -o -m 1 "UNKNOWN_TABLE"

echo "-- empty database session form"
$CLICKHOUSE_CLIENT_NO_DATABASE --user "${caller}" --query "EXECUTE AS ${target}; SELECT value FROM t;" 2>&1 | grep -o -m 1 "UNKNOWN_TABLE"

cleanup
