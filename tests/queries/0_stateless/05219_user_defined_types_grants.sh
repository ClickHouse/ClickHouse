#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: user-defined types live in a single process-wide namespace.

# `CREATE TYPE`, `DROP TYPE`, `SHOW TYPES` / `SHOW TYPE` and `system.user_defined_types` are gated by the
# `CREATE TYPE`, `DROP TYPE` and `SHOW USER DEFINED TYPES` privileges. Reading `system.user_defined_types`
# with only `SHOW USER DEFINED TYPES` relies on the implicit `SELECT` grant on that table, which matters
# when `select_from_system_db_requires_grant` is enabled (it is in CI).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER="user_05219_${CLICKHOUSE_DATABASE}"
TYPE="GrantedType_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${USER}"
$CLICKHOUSE_CLIENT --query "DROP TYPE IF EXISTS ${TYPE}"
$CLICKHOUSE_CLIENT --query "DROP TYPE IF EXISTS ${TYPE}2"
$CLICKHOUSE_CLIENT --query "CREATE USER ${USER} IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT --query "CREATE TYPE ${TYPE} AS UInt64"

function expect_denied()
{
    $CLICKHOUSE_CLIENT --user="${USER}" --query "$1" 2>&1 | grep -o -m1 "ACCESS_DENIED"
}

echo "--- no grants"
expect_denied "CREATE TYPE ${TYPE}2 AS String"
expect_denied "DROP TYPE ${TYPE}"
expect_denied "SHOW TYPES"
expect_denied "SHOW TYPE ${TYPE}"
expect_denied "SELECT name FROM system.user_defined_types WHERE name = '${TYPE}'"

echo "--- SHOW USER DEFINED TYPES"
$CLICKHOUSE_CLIENT --query "GRANT SHOW USER DEFINED TYPES ON *.* TO ${USER}"
$CLICKHOUSE_CLIENT --user="${USER}" --query "SHOW TYPES" | grep -c "^${TYPE}$"
$CLICKHOUSE_CLIENT --user="${USER}" --query "SHOW TYPE ${TYPE}" | sed "s/${CLICKHOUSE_DATABASE}/DB/g"
$CLICKHOUSE_CLIENT --user="${USER}" --query "SELECT name, base_type FROM system.user_defined_types WHERE name = '${TYPE}'" | sed "s/${CLICKHOUSE_DATABASE}/DB/g"
expect_denied "CREATE TYPE ${TYPE}2 AS String"
expect_denied "DROP TYPE ${TYPE}"

echo "--- CREATE TYPE"
$CLICKHOUSE_CLIENT --query "GRANT CREATE TYPE ON *.* TO ${USER}"
$CLICKHOUSE_CLIENT --user="${USER}" --query "CREATE TYPE ${TYPE}2 AS String"
$CLICKHOUSE_CLIENT --user="${USER}" --query "SHOW TYPE ${TYPE}2" | sed "s/${CLICKHOUSE_DATABASE}/DB/g"
expect_denied "DROP TYPE ${TYPE}"

echo "--- DROP TYPE"
$CLICKHOUSE_CLIENT --query "GRANT DROP TYPE ON *.* TO ${USER}"
$CLICKHOUSE_CLIENT --user="${USER}" --query "DROP TYPE ${TYPE}2"
$CLICKHOUSE_CLIENT --user="${USER}" --query "DROP TYPE ${TYPE}"
$CLICKHOUSE_CLIENT --user="${USER}" --query "SELECT count() FROM system.user_defined_types WHERE name LIKE '${TYPE}%'"

$CLICKHOUSE_CLIENT --query "DROP USER ${USER}"
