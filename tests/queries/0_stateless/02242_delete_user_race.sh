#!/usr/bin/env bash
# Tags: race, no-fasttest, no-parallel, no-backward-compatibility-check

# Test tries to reproduce a race between threads:
# - deletes user
# - creates user
# - uses it as session user
# - apply role to the user
#
# https://github.com/ClickHouse/ClickHouse/issues/35714

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
    DROP ROLE IF EXISTS test_role_02242;
    CREATE ROLE test_role_02242;
"

function delete_user()
{
    $CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS test_user_02242" ||:
}

function create_and_login_user()
{
    $CLICKHOUSE_CLIENT -q "CREATE USER IF NOT EXISTS test_user_02242" ||:
    $CLICKHOUSE_CLIENT -u "test_user_02242" -q "SELECT COUNT(*) FROM system.session_log WHERE user == 'test_user_02242'" > /dev/null ||:
}

function set_role()
{
    $CLICKHOUSE_CLIENT -q "SET ROLE test_role_02242 TO test_user_02242" ||:
}

export -f delete_user
export -f create_and_login_user
export -f set_role

TIMEOUT=10

for (( i = 0 ; i < 100; ++i ))
do
    clickhouse_client_loop_timeout $TIMEOUT create_and_login_user 2> /dev/null &
    clickhouse_client_loop_timeout $TIMEOUT delete_user 2> /dev/null &
    clickhouse_client_loop_timeout $TIMEOUT set_role 2> /dev/null &
done

wait

$CLICKHOUSE_CLIENT -q "DROP ROLE IF EXISTS test_role_02242"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS test_user_02242"
