#!/usr/bin/env bash
# Tags: race, no-fasttest, no-parallel

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

$CLICKHOUSE_CLIENT -m -q "
    DROP ROLE IF EXISTS test_role_02242;
    CREATE ROLE test_role_02242;
"

function delete_user()
{
    while true; do 
        $CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS test_user_02242" ||:
        sleep 0.$RANDOM; 
    done
}

function create_and_login_user()
{
    while true; do 
        $CLICKHOUSE_CLIENT -q "CREATE USER IF NOT EXISTS test_user_02242" ||:
        $CLICKHOUSE_CLIENT -u "test_user_02242" -q "SELECT COUNT(*) FROM system.session_log WHERE user == 'test_user_02242'" > /dev/null ||:
        sleep 0.$RANDOM; 
    done
}

function set_role()
{
    while true; do 
        $CLICKHOUSE_CLIENT -q "SET DEFAULT ROLE test_role_02242 TO test_user_02242" ||:
        sleep 0.$RANDOM; 
    done
}

export -f delete_user
export -f create_and_login_user
export -f set_role

TIMEOUT=10


timeout $TIMEOUT bash -c create_and_login_user 2> /dev/null &
timeout $TIMEOUT bash -c delete_user 2> /dev/null &
timeout $TIMEOUT bash -c set_role 2> /dev/null &

wait

$CLICKHOUSE_CLIENT -q "DROP ROLE IF EXISTS test_role_02242"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS test_user_02242"
