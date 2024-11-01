#!/usr/bin/env bash
# Tags: long, no-parallel, no-fasttest, no-debug

##################################################################################################
# Verify that login, logout, and login failure events are properly stored in system.session_log
# when different `IDENTIFIED BY` clauses are used on user.
#
# Make sure that system.session_log entries are non-empty and provide enough info on each event.
#
# Using multiple protocols
# * native TCP protocol with CH client
# * HTTP with CURL
# * MySQL - CH server accesses itself via mysql table function.
# * PostgreSQL - CH server accesses itself via postgresql table function, but can't execute query (No LOGIN SUCCESS entry).
# * gRPC - not done yet
#
# There is way to control how many time a query (e.g. via mysql table function) is retried
# and hence variable number of records in session_log. To mitigate this and simplify final query,
# each auth_type is tested for separate user. That way SELECT DISTINCT doesn't exclude log entries
# from different cases.
#
# All created users added to the ALL_USERNAMES and later cleaned up.
##################################################################################################

# To minimize amount of error context sent on failed queries when talking to CH via MySQL protocol.
export CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eu

# Since there is no way to cleanup system.session_log table,
# make sure that we can identify log entries from this test by a random user name.
BASE_USERNAME="session_log_test_user_$(tr -cd 'a-f0-9' < /dev/urandom | head -c 32)"
readonly BASE_USERNAME
TMP_QUERY_FILE=$(mktemp /tmp/tmp_query.log.XXXXXX)
readonly TMP_QUERY_FILE
declare -a ALL_USERNAMES
ALL_USERNAMES+=("${BASE_USERNAME}")

function reportError()
{
    if [ -s "${TMP_QUERY_FILE}" ] ;
    then
        echo "!!!!!! ERROR ${CLICKHOUSE_CLIENT} ${*} --queries-file ${TMP_QUERY_FILE}" >&2
        echo "query:" >&2
        cat "${TMP_QUERY_FILE}" >&2
        rm -f "${TMP_QUERY_FILE}"
    fi
}

function executeQuery()
{
    # Execute query (provided via heredoc or herestring) and print query in case of error.
    trap 'rm -f ${TMP_QUERY_FILE}; trap - ERR RETURN' RETURN
    # Since we want to report with current values supplied to this function call
    # shellcheck disable=SC2064
    trap "reportError $*" ERR

    cat - > "${TMP_QUERY_FILE}"
    ${CLICKHOUSE_CLIENT} "${@}" --queries-file "${TMP_QUERY_FILE}"
}

function cleanup()
{
    local usernames_to_cleanup
    usernames_to_cleanup="$(IFS=, ; echo "${ALL_USERNAMES[*]}")"
    executeQuery <<EOF
DROP USER IF EXISTS ${usernames_to_cleanup};
DROP SETTINGS PROFILE IF EXISTS session_log_test_profile;
DROP SETTINGS PROFILE IF EXISTS session_log_test_profile2;
DROP ROLE IF EXISTS session_log_test_role;
DROP ROLE IF EXISTS session_log_test_role2;
EOF
}

cleanup
trap "cleanup" EXIT

function executeQueryExpectError()
{
    cat - > "${TMP_QUERY_FILE}"
    ! ${CLICKHOUSE_CLIENT} --queries-file "${TMP_QUERY_FILE}" "${@}"  2>&1 | tee -a "${TMP_QUERY_FILE}"
}

function createUser()
{
    local auth_type="${1}"
    local username="${2}"
    local password="${3}"

    if [[ "${auth_type}" == "no_password" ]]
    then
        password=""

    elif [[ "${auth_type}" == "plaintext_password" ]]
    then
        # password="${password}"
        :

    elif [[ "${auth_type}" == "sha256_password" ]]
    then
        password="$(executeQuery <<< "SELECT hex(SHA256('${password}'))")"

    elif [[ "${auth_type}" == "double_sha1_password" ]]
    then
        password="$(executeQuery <<< "SELECT hex(SHA1(SHA1('${password}')))")"

    else
        echo "Invalid auth_type: ${auth_type}" >&2
        exit 1
    fi

    export RESULTING_PASS="${password}"
    if [ -n "${password}" ]
    then
        password="BY '${password}'"
    fi

    executeQuery <<EOF
DROP USER IF EXISTS '${username}';
CREATE USER '${username}' IDENTIFIED WITH ${auth_type} ${password};
GRANT SELECT ON system.one TO ${username};
GRANT SELECT ON INFORMATION_SCHEMA.* TO ${username};
EOF
    ALL_USERNAMES+=("${username}")
}

function testTCP()
{
    echo "TCP endpoint"

    local auth_type="${1}"
    local username="${2}"
    local password="${3}"

    # Loging\Logout
    if [[ -n "${password}" ]]
    then
        executeQuery -u "${username}" --password "${password}" <<< "SELECT 1 FORMAT Null;"
    else
        executeQuery -u "${username}" <<< "SELECT 1 FORMAT Null;" 
    fi

    # Wrong username
    executeQueryExpectError -u "invalid_${username}" \
        <<< "SELECT 1 Format Null" \
        | grep -Eq "Code: 516. .+ invalid_${username}: Authentication failed*"

    # Wrong password
    if [[ "${auth_type}" == "no_password" ]]
    then
        echo "TCP 'wrong password' case is skipped for ${auth_type}."
    else
        # user with `no_password` user is able to login with any password, so it makes sense to skip this testcase.
        executeQueryExpectError -u "${username}" --password  "invalid_${password}" \
            <<< "SELECT 1 Format Null"  \
            | grep -Eq "Code: 516. .+ ${username}: Authentication failed: password is incorrect, or there is no user with such name" 
    fi
}
   
function testHTTPWithURL()
{
    local auth_type="${1}"
    local username="${2}"
    local password="${3}"
    local clickhouse_url="${4}"

    # Loging\Logout
    ${CLICKHOUSE_CURL} -sS "${clickhouse_url}" \
        -H "X-ClickHouse-User: ${username}" -H "X-ClickHouse-Key: ${password}" \
        -d 'SELECT 1 Format Null'

    # Wrong username
    ${CLICKHOUSE_CURL} -sS "${clickhouse_url}" \
        -H "X-ClickHouse-User: invalid_${username}" -H "X-ClickHouse-Key: ${password}" \
        -d 'SELECT 1 Format Null' | grep -Eq "Code: 516. DB::Exception: invalid_${username}: Authentication failed: password is incorrect, or there is no user with such name"

    # Wrong password
    if [[ "${auth_type}" == "no_password" ]]
    then
        echo "HTTP 'wrong password' case is skipped for ${auth_type}."
    else
        # user with `no_password` is able to login with any password, so it makes sense to skip this testcase.
        ${CLICKHOUSE_CURL} -sS "${clickhouse_url}" \
            -H "X-ClickHouse-User: ${username}" -H "X-ClickHouse-Key: invalid_${password}" \
            -d 'SELECT 1 Format Null' \
            | grep -Eq "Code: 516. .+ ${username}: Authentication failed: password is incorrect, or there is no user with such name"
    fi
}

function testHTTP()
{
    echo "HTTP endpoint"
    testHTTPWithURL "${1}" "${2}" "${3}" "${CLICKHOUSE_URL}"
}

function testHTTPNamedSession()
{
    echo "HTTP endpoint with named session"
    local HTTP_SESSION_ID
    HTTP_SESSION_ID="session_id_$(tr -cd 'a-f0-9' < /dev/urandom | head -c 32)"
    if [ -v CLICKHOUSE_URL_PARAMS ]
    then
        CLICKHOUSE_URL_WITH_SESSION_ID="${CLICKHOUSE_URL}&session_id=${HTTP_SESSION_ID}"
    else
        CLICKHOUSE_URL_WITH_SESSION_ID="${CLICKHOUSE_URL}?session_id=${HTTP_SESSION_ID}"
    fi

    testHTTPWithURL "${1}" "${2}" "${3}" "${CLICKHOUSE_URL_WITH_SESSION_ID}"
}

function testMySQL()
{
    echo "MySQL endpoint ${auth_type}"
    local auth_type="${1}"
    local username="${2}"
    local password="${3}"

    trap "reportError" ERR

    # echo 'Loging\Logout'
    # sha256 auth is done differenctly for MySQL, so skip it for now.
    if [[ "${auth_type}" == "sha256_password" ]]
    then
        echo "MySQL 'successful login' case is skipped for ${auth_type}."
    else
        executeQuery \
            <<< "SELECT 1 FROM mysql('127.0.0.1:${CLICKHOUSE_PORT_MYSQL}', 'system', 'one', '${username}', '${password}') LIMIT 1 \
            FORMAT Null"
    fi

    echo 'Wrong username'
    executeQueryExpectError \
        <<< "SELECT 1 FROM mysql('127.0.0.1:${CLICKHOUSE_PORT_MYSQL}', 'system', 'one', 'invalid_${username}', '${password}') LIMIT 1 \
        FORMAT Null" \
        | grep -Eq "Code: 279\. DB::Exception: .* invalid_${username}"


    echo 'Wrong password'
    if [[ "${auth_type}" == "no_password" ]]
    then
        # user with `no_password` is able to login with any password, so it makes sense to skip this testcase.
        echo "MySQL 'wrong password' case is skipped for ${auth_type}."
    else
        executeQueryExpectError \
            <<< "SELECT 1 FROM mysql('127.0.0.1:${CLICKHOUSE_PORT_MYSQL}', 'system', 'one', '${username}', 'invalid_${password}') LIMIT 1 \
            FORMAT Null" | grep -Eq "Code: 279\. DB::Exception: .* ${username}"
    fi
}

 function testPostgreSQL()
 {
    echo "PostrgreSQL endpoint"
    local auth_type="${1}"

    if [[ "${auth_type}" == "sha256_password" || "${auth_type}" == "double_sha1_password" ]]
    then
        echo "PostgreSQL tests are skipped for ${auth_type}"
        return 0
    fi

    # TODO: Uncomment this case after implementation of postgresql function
    # Connecting to ClickHouse server
    ## Loging\Logout
    ## CH is being able to log into itself via PostgreSQL protocol but query fails.
    #executeQueryExpectError \
    #    <<< "SELECT 1 FROM postgresql('localhost:${CLICKHOUSE_PORT_POSTGRESQL', 'system', 'one', '${username}', '${password}') LIMIT 1 FORMAT Null" \

    # Wrong username
    executeQueryExpectError \
        <<< "SELECT 1 FROM postgresql('localhost:${CLICKHOUSE_PORT_POSTGRESQL}', 'system', 'one', 'invalid_${username}', '${password}') LIMIT 1 FORMAT Null" \
        | grep -Eq "Invalid user or password"

    if [[ "${auth_type}" == "no_password" ]]
    then
        # user with `no_password` is able to login with any password, so it makes sense to skip this testcase.
        echo "PostgreSQL 'wrong password' case is skipped for ${auth_type}."
    else
        # Wrong password
        executeQueryExpectError \
            <<< "SELECT 1 FROM postgresql('localhost:${CLICKHOUSE_PORT_POSTGRESQL}', 'system', 'one', '${username}', 'invalid_${password}') LIMIT 1 FORMAT Null" \
            | grep -Eq "Invalid user or password"
    fi
 }

function runEndpointTests()
{
    local case_name="${1}"
    shift 1

    local auth_type="${1}"
    local username="${2}"
    local password="${3}"
    local setup_queries="${4:-}"

    echo
    echo "#  ${auth_type} - ${case_name} "

    ${CLICKHOUSE_CLIENT} -q "SET log_comment='${username} ${auth_type} - ${case_name}';"
    if [[ -n "${setup_queries}" ]]
    then
        # echo "Executing setup queries: ${setup_queries}"
        echo "${setup_queries}" | executeQuery
    fi

    testTCP "${auth_type}" "${username}" "${password}"
    testHTTP "${auth_type}" "${username}" "${password}"

    testHTTPNamedSession "${auth_type}" "${username}" "${password}"
    testMySQL "${auth_type}" "${username}" "${password}"
    testPostgreSQL "${auth_type}" "${username}" "${password}"
}

function testAsUserIdentifiedBy()
{
    local auth_type="${1}"
    local password="password"

    cleanup

    local username="${BASE_USERNAME}_${auth_type}_no_profiles_no_roles"
    createUser "${auth_type}" "${username}" "${password}"
    runEndpointTests "No profiles no roles" "${auth_type}" "${username}" "${RESULTING_PASS}"

    username="${BASE_USERNAME}_${auth_type}_two_profiles_no_roles"
    createUser "${auth_type}" "${username}" "${password}"
    runEndpointTests "Two profiles, no roles" "${auth_type}" "${username}" "${RESULTING_PASS}" "\
DROP SETTINGS PROFILE IF EXISTS session_log_test_profile;
DROP SETTINGS PROFILE IF EXISTS session_log_test_profile2;
CREATE PROFILE session_log_test_profile SETTINGS max_memory_usage=10000000 TO ${username};
CREATE PROFILE session_log_test_profile2 SETTINGS max_rows_to_transfer=1000 TO ${username};
"

    username="${BASE_USERNAME}_${auth_type}_two_profiles_two_roles"
    createUser "${auth_type}" "${username}" "${password}"
    runEndpointTests "Two profiles and two simple roles" "${auth_type}" "${username}" "${RESULTING_PASS}" "\
CREATE ROLE session_log_test_role;
GRANT session_log_test_role TO ${username};
CREATE ROLE session_log_test_role2 SETTINGS max_columns_to_read=100;
GRANT session_log_test_role2 TO ${username};
SET DEFAULT ROLE session_log_test_role, session_log_test_role2 TO ${username};
"
}

# to cut off previous runs
start_time="$(executeQuery <<< 'SELECT now64(6);')"
readonly start_time

# Special case: user and profile are both defined in XML
runEndpointTests "User with profile from XML" "no_password" "session_log_test_xml_user" ''

testAsUserIdentifiedBy "no_password"
testAsUserIdentifiedBy "plaintext_password"
testAsUserIdentifiedBy "sha256_password"
testAsUserIdentifiedBy "double_sha1_password"

executeQuery <<EOF
SYSTEM FLUSH LOGS;

WITH
    now64(6) as n,
    toDateTime64('${start_time}', 3) as test_start_time
SELECT
    replaceAll(user, '${BASE_USERNAME}', '\${BASE_USERNAME}') as user_name,
    interface,
    type,
    if(count(*) > 1, 'many', toString(count(*))) -- do not rely on count value since MySQL does arbitrary number of retries
FROM
    system.session_log
WHERE
    (user LIKE '%session_log_test_xml_user%' OR user LIKE '%${BASE_USERNAME}%')
    AND
    event_time_microseconds >= test_start_time
GROUP BY
    user_name, interface, type
ORDER BY
    user_name, interface, type;
EOF
