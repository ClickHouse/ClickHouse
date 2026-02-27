#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user1="user1_${CLICKHOUSE_DATABASE}"
role1="role1_${CLICKHOUSE_DATABASE}"
role2="role2_${CLICKHOUSE_DATABASE}"
role3="role3_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
    DROP USER IF EXISTS $user1;
    DROP ROLE IF EXISTS $role1, $role2, $role3;
    "

$CLICKHOUSE_CLIENT --query "
    CREATE ROLE $role1;
    CREATE ROLE $role2;
    CREATE ROLE $role3;
"

echo A
$CLICKHOUSE_CLIENT --query "CREATE USER $user1 ROLE $role1, $role2"
$CLICKHOUSE_CLIENT --query "SHOW CREATE USER $user1"
$CLICKHOUSE_CLIENT --query "SELECT granted_role_name, granted_role_is_default FROM system.role_grants WHERE user_name='$user1' ORDER BY granted_role_name"
$CLICKHOUSE_CLIENT --query "GRANT $role3 TO $user1"
$CLICKHOUSE_CLIENT --query "SHOW CREATE USER $user1"
$CLICKHOUSE_CLIENT --query "SELECT granted_role_name, granted_role_is_default FROM system.role_grants WHERE user_name='$user1' ORDER BY granted_role_name"
$CLICKHOUSE_CLIENT --query "DROP USER $user1"
echo

echo B
$CLICKHOUSE_CLIENT --query "CREATE USER $user1 ROLE $role1, $role2 DEFAULT ROLE $role2"
$CLICKHOUSE_CLIENT --query "SHOW CREATE USER $user1"
$CLICKHOUSE_CLIENT --query "SELECT granted_role_name, granted_role_is_default FROM system.role_grants WHERE user_name='$user1' ORDER BY granted_role_name"
$CLICKHOUSE_CLIENT --query "GRANT $role3 TO $user1"
$CLICKHOUSE_CLIENT --query "SHOW CREATE USER $user1"
$CLICKHOUSE_CLIENT --query "SELECT granted_role_name, granted_role_is_default FROM system.role_grants WHERE user_name='$user1' ORDER BY granted_role_name"
$CLICKHOUSE_CLIENT --query "DROP USER $user1"
echo

echo C
$CLICKHOUSE_CLIENT --query "CREATE USER $user1 ROLE $role1, $role2 DEFAULT ROLE ALL EXCEPT $role2"
$CLICKHOUSE_CLIENT --query "SHOW CREATE USER $user1"
$CLICKHOUSE_CLIENT --query "SELECT granted_role_name, granted_role_is_default FROM system.role_grants WHERE user_name='$user1' ORDER BY granted_role_name"
$CLICKHOUSE_CLIENT --query "GRANT $role3 TO $user1"
$CLICKHOUSE_CLIENT --query "SHOW CREATE USER $user1"
$CLICKHOUSE_CLIENT --query "SELECT granted_role_name, granted_role_is_default FROM system.role_grants WHERE user_name='$user1' ORDER BY granted_role_name"
$CLICKHOUSE_CLIENT --query "SET DEFAULT ROLE $role3 TO $user1"
$CLICKHOUSE_CLIENT --query "SHOW CREATE USER $user1"
$CLICKHOUSE_CLIENT --query "SELECT granted_role_name, granted_role_is_default FROM system.role_grants WHERE user_name='$user1' ORDER BY granted_role_name"
$CLICKHOUSE_CLIENT --query "ALTER USER $user1 DEFAULT ROLE ALL"
$CLICKHOUSE_CLIENT --query "SHOW CREATE USER $user1"
$CLICKHOUSE_CLIENT --query "SELECT granted_role_name, granted_role_is_default FROM system.role_grants WHERE user_name='$user1' ORDER BY granted_role_name"
$CLICKHOUSE_CLIENT --query "ALTER USER $user1 ROLE $role1" 2>&1 | grep -o 'Syntax error' | uniq
$CLICKHOUSE_CLIENT --query "DROP USER $user1"
echo

echo D
$CLICKHOUSE_CLIENT --query "CREATE USER $user1 DEFAULT ROLE $role1, $role2"
$CLICKHOUSE_CLIENT --query "SHOW CREATE USER $user1"
$CLICKHOUSE_CLIENT --query "SELECT granted_role_name, granted_role_is_default FROM system.role_grants WHERE user_name='$user1' ORDER BY granted_role_name"
$CLICKHOUSE_CLIENT --query "GRANT $role3 TO $user1"
$CLICKHOUSE_CLIENT --query "SHOW CREATE USER $user1"
$CLICKHOUSE_CLIENT --query "SELECT granted_role_name, granted_role_is_default FROM system.role_grants WHERE user_name='$user1' ORDER BY granted_role_name"
$CLICKHOUSE_CLIENT --query "DROP USER $user1"
echo

echo E
$CLICKHOUSE_CLIENT --query "CREATE USER $user1 ROLE $role1, $role2 DEFAULT ROLE $role3" 2>&1 | grep -o 'SET_NON_GRANTED_ROLE' | uniq
$CLICKHOUSE_CLIENT --query "CREATE USER $user1 ROLE ALL" 2>&1 | grep -o 'Syntax error' | uniq

$CLICKHOUSE_CLIENT --query "DROP ROLE IF EXISTS $role1, $role2, $role3"
