#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

QUERY_ID_PREFIX="02832_alter_max_sessions_query_$$"
PROFILE="02832_alter_max_sessions_profile_$$"
USER="02832_alter_max_sessions_user_$$"
USER2="02832_alter_max_sessions_user_two_$$"
ROLE="02832_alter_max_sessions_role_$$"

${CLICKHOUSE_CLIENT} -q $"DROP USER IF EXISTS '${USER}'"
${CLICKHOUSE_CLIENT} -q $"DROP PROFILE IF EXISTS ${PROFILE}"
${CLICKHOUSE_CLIENT} -q $"CREATE SETTINGS PROFILE ${PROFILE}"
${CLICKHOUSE_CLIENT} -q $"CREATE USER '${USER}' SETTINGS PROFILE '${PROFILE}'"

function wait_for_query_to_start()
{
    while [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$1'") == 0 ]]; do sleep 0.2; done
}

function test_alter_max_sessions_for_user()
{
    local max_sessions_for_user="$1"
    echo $"test_alter_profile case: max_sessions_for_user ${max_sessions_for_user}"

    # Step 0: Set max_sessions_for_user.
    ${CLICKHOUSE_CLIENT} -q $"ALTER SETTINGS PROFILE ${PROFILE} SETTINGS max_sessions_for_user = ${max_sessions_for_user}"

    # Step 1: Simulaneously run `max_sessions_for_user` queries. These queries should run without any problems.
    for ((i = 1 ; i <= max_sessions_for_user ; i++)); do
        local query_id="${QUERY_ID_PREFIX}_${i}_${max_sessions_for_user}"
        ${CLICKHOUSE_CLIENT} --max_block_size 1 --query_id $query_id --user $USER --function_sleep_max_microseconds_per_block=120000000 -q "SELECT sleepEachRow(0.1) FROM numbers(1200)" &>/dev/null &
        wait_for_query_to_start $query_id
    done

    # Step 2: Run another `max_sessions_for_user` + 1 query. That query should fail.
    local query_id="${QUERY_ID_PREFIX}_should_fail"
    ${CLICKHOUSE_CLIENT} --query_id $query_id --user $USER -q "SELECT 1" 2>&1 | grep -o -m 1 'USER_SESSION_LIMIT_EXCEEDED'
    
    # Step 3: Stop running queries launched at step 1.
    for ((i = 1 ; i <= max_sessions_for_user ; i++)); do
        local query_id="${QUERY_ID_PREFIX}_${i}_${max_sessions_for_user}"
        $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id='$query_id' ASYNC" >/dev/null
    done

    wait
}

test_alter_max_sessions_for_user 1
test_alter_max_sessions_for_user 2

${CLICKHOUSE_CLIENT} -q "SELECT 1 SETTINGS max_sessions_for_user = 1" 2>&1 | grep -m 1 -o 'READONLY' | head -1
${CLICKHOUSE_CLIENT} -q $"SET max_sessions_for_user = 1 " 2>&1 | grep -o  -m 1 'READONLY' | head -1
${CLICKHOUSE_CLIENT} --max_sessions_for_user=1 -q $"SELECT 1 " 2>&1 | grep -o  -m 1 'READONLY' | head -1
# max_sessions_for_user is profile setting
${CLICKHOUSE_CLIENT} -q $"CREATE USER ${USER2} SETTINGS max_sessions_for_user = 1 " 2>&1 | grep -o  -m 1 'READONLY' | head -1
${CLICKHOUSE_CLIENT} -q $"ALTER USER ${USER} SETTINGS max_sessions_for_user = 1" 2>&1 | grep -o  -m 1 'READONLY' | head -1
${CLICKHOUSE_CLIENT} -q $"CREATE ROLE ${ROLE} SETTINGS max_sessions_for_user = 1" 2>&1 | grep -o  -m 1 'READONLY' | head -1
${CLICKHOUSE_CLIENT} -q $"CREATE ROLE ${ROLE}"
${CLICKHOUSE_CLIENT} -q $"ALTER ROLE ${ROLE} SETTINGS max_sessions_for_user = 1 " 2>&1 | grep -o  -m 1 'READONLY' | head -1

${CLICKHOUSE_CLIENT} -q $"DROP USER IF EXISTS '${USER}'"
${CLICKHOUSE_CLIENT} -q $"DROP USER IF EXISTS '${USER2}'"
${CLICKHOUSE_CLIENT} -q $"DROP PROFILE IF EXISTS ${PROFILE}"
${CLICKHOUSE_CLIENT} -q $"DROP ROLE IF EXISTS ${ROLE}"
