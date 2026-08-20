#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user_name="user_03822_${CLICKHOUSE_DATABASE}"
role_name="role_03822_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user_name}"
$CLICKHOUSE_CLIENT -q "DROP ROLE IF EXISTS ${role_name}"

$CLICKHOUSE_CLIENT -q "CREATE USER ${user_name}"
$CLICKHOUSE_CLIENT -q "CREATE ROLE ${role_name}"

$CLICKHOUSE_CLIENT -q "GRANT ${role_name} TO ${user_name}"
$CLICKHOUSE_CLIENT -q "SET DEFAULT ROLE ${role_name} TO ${user_name}"
$CLICKHOUSE_CLIENT -q "SHOW CREATE USER ${user_name}"
$CLICKHOUSE_CLIENT --user ${user_name} -q "SHOW CURRENT ROLES"

echo "After revoke:"
$CLICKHOUSE_CLIENT -q "REVOKE ${role_name} FROM ${user_name}"
$CLICKHOUSE_CLIENT -q "SHOW CREATE USER ${user_name}"
$CLICKHOUSE_CLIENT --user ${user_name} -q "SHOW CURRENT ROLES"

$CLICKHOUSE_CLIENT -q "DROP USER ${user_name}"
$CLICKHOUSE_CLIENT -q "DROP ROLE ${role_name}"
