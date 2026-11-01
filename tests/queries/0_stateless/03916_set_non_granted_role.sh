#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user_name="user_${CLICKHOUSE_DATABASE}"
role_name="role_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
    DROP USER IF EXISTS ${user_name};
    DROP ROLE IF EXISTS ${role_name};
"

$CLICKHOUSE_CLIENT --query "
    CREATE USER ${user_name};
    CREATE ROLE ${role_name};
"

output=$($CLICKHOUSE_CLIENT --query "SET DEFAULT ROLE ${role_name} TO ${user_name}" 2>&1)
status=$?

[ $status -eq 0 ] && echo "Successful" || echo "Failed"
echo "$output" | grep -o 'SET_NON_GRANTED_ROLE' | uniq

$CLICKHOUSE_CLIENT --query "
    DROP USER ${user_name};
    DROP ROLE ${role_name};
"
