#!/usr/bin/env bash
# Tags: no-debug

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

readonly PID=$$

readonly TEST_USER="02835_USER_${PID}"
readonly TEST_ROLE="02835_ROLE_${PID}"
readonly TEST_PROFILE="02835_PROFILE_${PID}"
readonly SESSION_LOG_MATCHING_FIELDS="auth_id, auth_type, client_version_major, client_version_minor, client_version_patch, interface"

function tcp_session()
{
    local user=$1
    ${CLICKHOUSE_CLIENT} -q "SELECT COUNT(*) FROM system.numbers" --user="${user}"
}

function http_session()
{
    local user=$1
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user}&password=pass" -d "SELECT COUNT(*) FROM system.numbers"
}

function http_with_session_id_session()
{
    local user=$1
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user}&password=pass" -d "SELECT COUNT(*) FROM system.numbers"
}

# Busy-waits until user $1, specified amount of queries ($2) will run simultaneously.
function wait_for_queries_start()
{
    local user=$1
    local queries_count=$2
    # 10 seconds waiting
    counter=0 retries=100
    while [[ $counter -lt $retries ]]; do
        result=$($CLICKHOUSE_CLIENT --query "SELECT COUNT(*) FROM system.processes WHERE user = '${user}'")
        if [[ $result == "${queries_count}" ]]; then
            break;
        fi
        sleep 0.1
        ((++counter))
    done
}

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS"
${CLICKHOUSE_CLIENT} -q "DELETE FROM system.session_log WHERE user = '${TEST_USER}'"

# DROP USE CASE
${CLICKHOUSE_CLIENT} -q "CREATE USER IF NOT EXISTS ${TEST_USER}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON system.numbers TO ${TEST_USER}"

export -f tcp_session;
export -f http_session;
export -f http_with_session_id_session;

timeout 10s bash -c "tcp_session ${TEST_USER}" >/dev/null 2>&1 &
timeout 10s bash -c "http_session ${TEST_USER}" >/dev/null 2>&1 &
timeout 10s bash -c "http_with_session_id_session ${TEST_USER}" >/dev/null 2>&1 &

wait_for_queries_start $TEST_USER 3
${CLICKHOUSE_CLIENT} -q "DROP USER ${TEST_USER}"
${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE user = '${TEST_USER}' SYNC" >/dev/null &

wait

# DROP ROLE CASE
${CLICKHOUSE_CLIENT} -q "CREATE ROLE IF NOT EXISTS ${TEST_ROLE}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${TEST_USER} DEFAULT ROLE ${TEST_ROLE}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON system.numbers TO ${TEST_USER}"

timeout 10s bash -c "tcp_session ${TEST_USER}" >/dev/null 2>&1 &
timeout 10s bash -c "http_session ${TEST_USER}" >/dev/null 2>&1 &
timeout 10s bash -c "http_with_session_id_session ${TEST_USER}" >/dev/null 2>&1 &

wait_for_queries_start $TEST_USER 3
${CLICKHOUSE_CLIENT} -q "DROP ROLE ${TEST_ROLE}"
${CLICKHOUSE_CLIENT} -q "DROP USER ${TEST_USER}"

${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE user = '${TEST_USER}' SYNC" >/dev/null &

wait

# DROP PROFILE CASE
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE IF NOT EXISTS '${TEST_PROFILE}'"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${TEST_USER} SETTINGS PROFILE '${TEST_PROFILE}'"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON system.numbers TO ${TEST_USER}"

timeout 10s bash -c "tcp_session ${TEST_USER}" >/dev/null 2>&1 &
timeout 10s bash -c "http_session ${TEST_USER}" >/dev/null 2>&1 &
timeout 10s bash -c "http_with_session_id_session ${TEST_USER}" >/dev/null 2>&1 &

wait_for_queries_start $TEST_USER 3
${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE '${TEST_PROFILE}'"
${CLICKHOUSE_CLIENT} -q "DROP USER ${TEST_USER}"

${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE user = '${TEST_USER}' SYNC" >/dev/null &

wait

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS"

echo "port_0_sessions:"
${CLICKHOUSE_CLIENT} -q "SELECT count(*) FROM system.session_log WHERE user = '${TEST_USER}' AND client_port = 0"
echo "address_0_sessions:"
${CLICKHOUSE_CLIENT} -q "SELECT count(*) FROM system.session_log WHERE user = '${TEST_USER}' AND client_address = toIPv6('::')"
echo "Corresponding LoginSuccess/Logout"

# The client can exit sooner than the server records its disconnection and closes the session.
# When the client disconnects, two processes happen at the same time and are in the race condition:
# - the client application exits and returns control to the shell;
# - the server closes the session and records the logout event to the session log.
# We cannot expect that after the control is returned to the shell, the server records the logout event.
while true
do
    [[ 9 -eq $(${CLICKHOUSE_CLIENT} -q "
        SELECT COUNT(*) FROM (
            SELECT ${SESSION_LOG_MATCHING_FIELDS} FROM system.session_log WHERE user = '${TEST_USER}' AND type = 'LoginSuccess'
            INTERSECT
            SELECT ${SESSION_LOG_MATCHING_FIELDS}, FROM system.session_log WHERE user = '${TEST_USER}' AND type = 'Logout'
        )") ]] && echo 9 && break;
    sleep 0.1
done

echo "LoginFailure"
${CLICKHOUSE_CLIENT} -q "SELECT COUNT(*) FROM system.session_log WHERE user = '${TEST_USER}' AND type = 'LoginFailure'"
