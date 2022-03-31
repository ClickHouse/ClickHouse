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
# shellcheck source=./replication.lib
. "$CURDIR"/replication.lib

#export PS4='\nDEBUG level:$SHLVL subshell-level: $BASH_SUBSHELL \nsource-file:${BASH_SOURCE} line#:${LINENO} function:${FUNCNAME[0]:+${FUNCNAME[0]}(): }\nstatement: '


$CLICKHOUSE_CLIENT -nm -q "
    DROP ROLE IF EXISTS test_role_02242;
    CREATE ROLE test_role_02242;
"

readonly REPEAT=1000

function delete_user()
{
    local i
    for (( i = 0; i < REPEAT; ++i ))
    do
        $CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS test_user_02242" ||:
    done
}

function create_and_login_user()
{
    local i
    for (( i = 0; i < REPEAT; ++i ))
    do
        $CLICKHOUSE_CLIENT -q "CREATE USER IF NOT EXISTS test_user_02242" ||:
        $CLICKHOUSE_CLIENT -u "test_user_02242" -q "SELECT version()" > /dev/null ||:
    done
}

function set_role()
{
    local i
    for (( i = 0; i < REPEAT; ++i ))
    do
        $CLICKHOUSE_CLIENT -q "SET ROLE test_role_02242 TO test_user_02242" ||:
    done
}

export -f delete_user
export -f create_and_login_user
export -f set_role


TIMEOUT=0.1

for (( i = 0 ; i < 1000; ++i ))
do
    clickhouse_client_loop_timeout $TIMEOUT create_and_login_user 2> /dev/null &
    clickhouse_client_loop_timeout $TIMEOUT delete_user 2> /dev/null &
    clickhouse_client_loop_timeout $TIMEOUT login_user 2> /dev/null &
    clickhouse_client_loop_timeout $TIMEOUT set_role 2> /dev/null &
done

wait

# $CLICKHOUSE_CLIENT -q "DROP ROLE IF EXISTS test_role_02242"
# $CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS test_user_02242"

# wait
