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
        SHOW CURRENT ROLES
    "
}

check_error()
{
    local query="$1"
    local expected="$2"

    local output
    output=$($CLICKHOUSE_CLIENT --query "$query" 2>&1 || true)
    echo "$output" | grep -o "$expected" | uniq
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
check_error "SET DEFAULT ROLE NONE TO ${role_x}" "UNKNOWN_USER"
check_error "SET DEFAULT ROLE ${role_x} TO ${role_y}" "UNKNOWN_USER"
check_error "SET DEFAULT ROLE ${user_name} TO ${user_name}" "UNKNOWN_ROLE"
check_error "ALTER USER ${user_name} DEFAULT ROLE ${user_name}" "UNKNOWN_ROLE"
check_error "ALTER USER ${user_name} DEFAULT ROLE ALL EXCEPT ${user_name}" "UNKNOWN_ROLE"
