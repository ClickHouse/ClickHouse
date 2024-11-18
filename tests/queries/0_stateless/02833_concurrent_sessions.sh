#!/usr/bin/env bash
# Tags: no-fasttest, no-debug

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

readonly PID=$$

# Each user uses a separate thread.
readonly TCP_USERS=( "02833_TCP_USER_${PID}"_{1,2} ) # 2 concurrent TCP users
readonly HTTP_USERS=( "02833_HTTP_USER_${PID}" )
readonly HTTP_WITH_SESSION_ID_SESSION_USERS=( "02833_HTTP_WITH_SESSION_ID_USER_${PID}" )
readonly MYSQL_USERS=( "02833_MYSQL_USER_${PID}")
readonly ALL_USERS=( "${TCP_USERS[@]}" "${HTTP_USERS[@]}" "${HTTP_WITH_SESSION_ID_SESSION_USERS[@]}" "${MYSQL_USERS[@]}" )

TCP_USERS_SQL_COLLECTION_STRING="$( echo "${TCP_USERS[*]}" | sed "s/[^[:space:]]\+/'&'/g" | sed 's/[[:space:]]/,/g' )"
readonly TCP_USERS_SQL_COLLECTION_STRING
HTTP_USERS_SQL_COLLECTION_STRING="$( echo "${HTTP_USERS[*]}" | sed "s/[^[:space:]]\+/'&'/g" | sed 's/[[:space:]]/,/g' )"
readonly HTTP_USERS_SQL_COLLECTION_STRING
HTTP_WITH_SESSION_ID_USERS_SQL_COLLECTION_STRING="$( echo "${HTTP_WITH_SESSION_ID_SESSION_USERS[*]}" | sed "s/[^[:space:]]\+/'&'/g" | sed 's/[[:space:]]/,/g' )"
readonly HTTP_WITH_SESSION_ID_USERS_SQL_COLLECTION_STRING
MYSQL_USERS_SQL_COLLECTION_STRING="$( echo "${MYSQL_USERS[*]}" | sed "s/[^[:space:]]\+/'&'/g" | sed 's/[[:space:]]/,/g' )"
readonly MYSQL_USERS_SQL_COLLECTION_STRING
ALL_USERS_SQL_COLLECTION_STRING="$( echo "${ALL_USERS[*]}" | sed "s/[^[:space:]]\+/'&'/g" | sed 's/[[:space:]]/,/g' )"
readonly ALL_USERS_SQL_COLLECTION_STRING

readonly SESSION_LOG_MATCHING_FIELDS="auth_id, auth_type, client_version_major, client_version_minor, client_version_patch, interface"

for user in "${ALL_USERS[@]}"; do
    ${CLICKHOUSE_CLIENT} -q "CREATE USER IF NOT EXISTS ${user} IDENTIFIED WITH plaintext_password BY 'pass'"
    ${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON system.* TO ${user}"
    ${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON INFORMATION_SCHEMA.* TO ${user}";
done

# All <type>_session functions execute in separate threads.
# These functions try to create a session with successful login and logout.
# Sleep a small, random amount of time to make concurrency more intense.
# and try to login with an invalid password.
function tcp_session()
{
    local user=$1
    local i=0
    while (( (i++) < 3 )); do
        # login logout
        ${CLICKHOUSE_CLIENT} -q "SELECT 1, sleep(0.01${RANDOM})" --user="${user}" --password="pass"
        # login failure
        ${CLICKHOUSE_CLIENT} -q "SELECT 2" --user="${user}" --password 'invalid'
    done
}

function http_session()
{
    local user=$1
    local i=0
    while (( (i++) < 3 )); do
        # login logout
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user}&password=pass" -d "SELECT 3, sleep(0.01${RANDOM})"

        # login failure
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user}&password=wrong" -d "SELECT 4"
    done
}

function http_with_session_id_session()
{
    local user=$1
    local i=0
    while (( (i++) < 3 )); do
        # login logout
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${user}&user=${user}&password=pass" -d "SELECT 5, sleep 0.01${RANDOM}"

        # login failure
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${user}&user=${user}&password=wrong" -d "SELECT 6"
    done
}

function mysql_session()
{
    local user=$1
    local i=0
    while (( (i++) < 3 )); do
        # login logout
        ${CLICKHOUSE_CLIENT} -q "SELECT 1, sleep(0.01${RANDOM}) FROM mysql('127.0.0.1:9004', 'system', 'one', '${user}', 'pass')"

        # login failure
        ${CLICKHOUSE_CLIENT} -q "SELECT 1 FROM mysql('127.0.0.1:9004', 'system', 'one', '${user}', 'wrong', SETTINGS connection_max_tries=1)"
    done
}

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS"
${CLICKHOUSE_CLIENT} -q "DELETE FROM system.session_log WHERE user IN (${ALL_USERS_SQL_COLLECTION_STRING})"

export -f tcp_session;
export -f http_session;
export -f http_with_session_id_session;
export -f mysql_session;

for user in "${TCP_USERS[@]}"; do
    tcp_session ${user} >/dev/null 2>&1 &
done

for user in "${HTTP_USERS[@]}"; do
    http_session ${user} >/dev/null 2>&1 &
done

for user in "${HTTP_WITH_SESSION_ID_SESSION_USERS[@]}"; do
    http_with_session_id_session ${user} >/dev/null 2>&1 &
done

for user in "${MYSQL_USERS[@]}"; do
    mysql_session ${user} >/dev/null 2>&1 &
done

wait

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS"

echo "sessions:"
${CLICKHOUSE_CLIENT} -q "SELECT count(*) FROM system.session_log WHERE user IN (${ALL_USERS_SQL_COLLECTION_STRING})"

echo "port_0_sessions:"
${CLICKHOUSE_CLIENT} -q "SELECT count(*) FROM system.session_log WHERE user IN (${ALL_USERS_SQL_COLLECTION_STRING}) AND client_port = 0"

echo "address_0_sessions:"
${CLICKHOUSE_CLIENT} -q "SELECT count(*) FROM system.session_log WHERE user IN (${ALL_USERS_SQL_COLLECTION_STRING}) AND client_address = toIPv6('::')"

echo "tcp_sessions"
${CLICKHOUSE_CLIENT} -q "SELECT count(*) FROM system.session_log WHERE user IN (${TCP_USERS_SQL_COLLECTION_STRING}) AND interface = 'TCP'"
echo "http_sessions"
${CLICKHOUSE_CLIENT} -q "SELECT count(*) FROM system.session_log WHERE user IN (${HTTP_USERS_SQL_COLLECTION_STRING}) AND interface = 'HTTP'"
echo "http_with_session_id_sessions"
${CLICKHOUSE_CLIENT} -q "SELECT count(*) FROM system.session_log WHERE user IN (${HTTP_WITH_SESSION_ID_USERS_SQL_COLLECTION_STRING}) AND interface = 'HTTP'"
echo "mysql_sessions"
${CLICKHOUSE_CLIENT} -q "SELECT count(*) FROM system.session_log WHERE user IN (${MYSQL_USERS_SQL_COLLECTION_STRING}) AND interface = 'MySQL'"

for user in "${ALL_USERS[@]}"; do
    ${CLICKHOUSE_CLIENT} -q "DROP USER ${user}"
    echo "Corresponding LoginSuccess/Logout"

    # The client can exit sooner than the server records its disconnection and closes the session.
    # When the client disconnects, two processes happen at the same time and are in the race condition:
    # - the client application exits and returns control to the shell;
    # - the server closes the session and records the logout event to the session log.
    # We cannot expect that after the control is returned to the shell, the server records the logout event.
    while true
    do
        [[ 3 -eq $(${CLICKHOUSE_CLIENT} -q "
            SELECT COUNT(*) FROM (
                SELECT ${SESSION_LOG_MATCHING_FIELDS} FROM system.session_log WHERE user = '${user}' AND type = 'LoginSuccess'
                INTERSECT
                SELECT ${SESSION_LOG_MATCHING_FIELDS} FROM system.session_log WHERE user = '${user}' AND type = 'Logout'
            )") ]] && echo 3 && break;
        sleep 0.1
    done

    echo "LoginFailure"
    ${CLICKHOUSE_CLIENT} -q "SELECT COUNT(*) FROM system.session_log WHERE user = '${user}' AND type = 'LoginFailure'"
 done
