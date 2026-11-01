#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

user_name="user_04509_${CLICKHOUSE_DATABASE}"
role_x="role_x_04509_${CLICKHOUSE_DATABASE}"
role_y="role_y_04509_${CLICKHOUSE_DATABASE}"

cleanup()
{
    $CLICKHOUSE_CLIENT --query "
        DROP USER IF EXISTS ${user_name};
        DROP ROLE IF EXISTS ${role_x}, ${role_y};
    "
}

show_current_roles()
{
    $CLICKHOUSE_CLIENT --user "${user_name}" --query "
        SELECT
            groupArray(role_name),
            groupArray(with_admin_option),
            groupArray(is_default)
        FROM
        (
            SELECT
                role_name,
                with_admin_option,
                is_default
            FROM system.current_roles
            ORDER BY role_name
        )
    "
}

trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT --query "
    CREATE USER ${user_name};
    CREATE ROLE ${role_x};
    CREATE ROLE ${role_y};
"

echo "set_default_role"
show_current_roles
$CLICKHOUSE_CLIENT --query "GRANT ${role_x}, ${role_y} TO ${user_name}"
show_current_roles
$CLICKHOUSE_CLIENT --query "SET DEFAULT ROLE NONE TO ${user_name}"
show_current_roles
$CLICKHOUSE_CLIENT --query "SET DEFAULT ROLE ${role_x} TO ${user_name}"
show_current_roles
$CLICKHOUSE_CLIENT --query "SET DEFAULT ROLE ${role_y} TO ${user_name}"
show_current_roles
$CLICKHOUSE_CLIENT --query "SET DEFAULT ROLE ALL TO ${user_name}"
show_current_roles
$CLICKHOUSE_CLIENT --query "SET DEFAULT ROLE ALL EXCEPT ${role_x} TO ${user_name}"
show_current_roles
echo

echo "alter_user"
$CLICKHOUSE_CLIENT --query "CREATE USER OR REPLACE ${user_name}"
show_current_roles
$CLICKHOUSE_CLIENT --query "GRANT ${role_x}, ${role_y} TO ${user_name}"
show_current_roles
$CLICKHOUSE_CLIENT --query "ALTER USER ${user_name} DEFAULT ROLE NONE"
show_current_roles
$CLICKHOUSE_CLIENT --query "ALTER USER ${user_name} DEFAULT ROLE ${role_x}"
show_current_roles
$CLICKHOUSE_CLIENT --query "ALTER USER ${user_name} DEFAULT ROLE ALL"
show_current_roles
$CLICKHOUSE_CLIENT --query "ALTER USER ${user_name} DEFAULT ROLE ALL EXCEPT ${role_x}"
show_current_roles
echo

echo "errors"
$CLICKHOUSE_CLIENT --query "SET DEFAULT ROLE NONE TO ${role_x}; -- { serverError UNKNOWN_USER }"
echo "UNKNOWN_USER"
$CLICKHOUSE_CLIENT --query "SET DEFAULT ROLE ${role_x} TO ${role_y}; -- { serverError UNKNOWN_USER }"
echo "UNKNOWN_USER"
$CLICKHOUSE_CLIENT --query "SET DEFAULT ROLE ${user_name} TO ${user_name}; -- { serverError UNKNOWN_ROLE }"
echo "UNKNOWN_ROLE"
$CLICKHOUSE_CLIENT --query "ALTER USER ${user_name} DEFAULT ROLE ${user_name}; -- { serverError UNKNOWN_ROLE }"
echo "UNKNOWN_ROLE"
$CLICKHOUSE_CLIENT --query "ALTER USER ${user_name} DEFAULT ROLE ALL EXCEPT ${user_name}; -- { serverError UNKNOWN_ROLE }"
echo "UNKNOWN_ROLE"
