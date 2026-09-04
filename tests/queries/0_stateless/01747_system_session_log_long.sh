#!/usr/bin/env bash
# Tags: long, no-fasttest, no-debug, no-openssl-fips
# fips: SHA1 is not available in FIPS mode

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
# The profile/role dimension (no profiles/roles, two profiles, two profiles and two roles) is only
# exercised for `plaintext_password` since the final aggregation groups by (user, interface, type)
# and never asserts on profiles/roles - so it is independent of auth_type in the oracle. The other
# auth types only run the baseline (no profiles, no roles) case.
#
# All created users added to the ALL_USERNAMES and later cleaned up.
#
# Case blocks below are independent (unique users/profiles/roles per case) and are run as
# backgrounded jobs to cut down on wall-clock time; their captured output is printed afterwards in
# a fixed order so the reference stays deterministic.
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

# Fixed, precomputed identities for each remaining case. Cases run concurrently as backgrounded
# jobs, so every user/profile/role name below must be unique across cases.
XML_USERNAME="session_log_test_xml_user"
readonly XML_USERNAME
NO_PASSWORD_USERNAME="${BASE_USERNAME}_no_password_no_profiles_no_roles"
readonly NO_PASSWORD_USERNAME
PLAINTEXT_BASELINE_USERNAME="${BASE_USERNAME}_plaintext_password_no_profiles_no_roles"
readonly PLAINTEXT_BASELINE_USERNAME
PLAINTEXT_PROFILES_USERNAME="${BASE_USERNAME}_plaintext_password_two_profiles_no_roles"
readonly PLAINTEXT_PROFILES_USERNAME
PLAINTEXT_ROLES_USERNAME="${BASE_USERNAME}_plaintext_password_two_profiles_two_roles"
readonly PLAINTEXT_ROLES_USERNAME
SHA256_USERNAME="${BASE_USERNAME}_sha256_password_no_profiles_no_roles"
readonly SHA256_USERNAME
DOUBLE_SHA1_USERNAME="${BASE_USERNAME}_double_sha1_password_no_profiles_no_roles"
readonly DOUBLE_SHA1_USERNAME

PROFILE_NAME_1="${PLAINTEXT_PROFILES_USERNAME}_profile1"
readonly PROFILE_NAME_1
PROFILE_NAME_2="${PLAINTEXT_PROFILES_USERNAME}_profile2"
readonly PROFILE_NAME_2
ROLE_NAME_1="${PLAINTEXT_ROLES_USERNAME}_role1"
readonly ROLE_NAME_1
ROLE_NAME_2="${PLAINTEXT_ROLES_USERNAME}_role2"
readonly ROLE_NAME_2

declare -a ALL_USERNAMES
# NB: XML_USERNAME is intentionally excluded - it is defined in the server XML config, not via
# CREATE USER, so it must not be dropped.
ALL_USERNAMES+=(
    "${BASE_USERNAME}"
    "${NO_PASSWORD_USERNAME}"
    "${PLAINTEXT_BASELINE_USERNAME}"
    "${PLAINTEXT_PROFILES_USERNAME}"
    "${PLAINTEXT_ROLES_USERNAME}"
    "${SHA256_USERNAME}"
    "${DOUBLE_SHA1_USERNAME}"
)

function reportError()
{
    # tmp_query_file may be omitted (e.g. testMySQL's own bare "trap reportError ERR"), so default
    # to empty rather than tripping "set -u" on the missing positional parameter.
    local tmp_query_file="${1:-}"
    shift || :
    if [ -n "${tmp_query_file}" ] && [ -s "${tmp_query_file}" ] ;
    then
        echo "!!!!!! ERROR ${CLICKHOUSE_CLIENT} ${*} --queries-file ${tmp_query_file}" >&2
        echo "query:" >&2
        cat "${tmp_query_file}" >&2
        rm -f "${tmp_query_file}"
    fi
}

function executeQuery()
{
    # Execute query (provided via heredoc or herestring) and print query in case of error.
    # Every call gets its own temp file since cases run concurrently as backgrounded jobs.
    local tmp_query_file
    tmp_query_file=$(mktemp /tmp/tmp_query.log.XXXXXX)
    trap 'rm -f ${tmp_query_file}; trap - ERR RETURN' RETURN
    # Since we want to report with current values supplied to this function call
    # shellcheck disable=SC2064
    trap "reportError ${tmp_query_file} $*" ERR

    cat - > "${tmp_query_file}"
    ${CLICKHOUSE_CLIENT} "${@}" --queries-file "${tmp_query_file}"
}

function cleanup()
{
    local usernames_to_cleanup
    usernames_to_cleanup="$(IFS=, ; echo "${ALL_USERNAMES[*]}")"
    executeQuery <<EOF
DROP USER IF EXISTS ${usernames_to_cleanup};
DROP SETTINGS PROFILE IF EXISTS ${PROFILE_NAME_1};
DROP SETTINGS PROFILE IF EXISTS ${PROFILE_NAME_2};
DROP ROLE IF EXISTS ${ROLE_NAME_1};
DROP ROLE IF EXISTS ${ROLE_NAME_2};
EOF
}

cleanup
trap "cleanup" EXIT

function executeQueryExpectError()
{
    local tmp_query_file
    tmp_query_file=$(mktemp /tmp/tmp_query.log.XXXXXX)
    cat - > "${tmp_query_file}"
    ! ${CLICKHOUSE_CLIENT} --queries-file "${tmp_query_file}" "${@}"  2>&1 | tee -a "${tmp_query_file}"
    rm -f "${tmp_query_file}"
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

    local username="${2}"
    local password="${3}"

    local http_session_id
    http_session_id="session_id_$(tr -cd 'a-f0-9' < /dev/urandom | head -c 32)"
    local clickhouse_url_with_session_id="${CLICKHOUSE_URL}&session_id=${http_session_id}"

    # Login\Logout only. Wrong-username/wrong-password checks are intentionally skipped here:
    # authentication failure happens before a named session is established, so it is
    # indistinguishable from (and already covered by) the plain testHTTP checks above.
    ${CLICKHOUSE_CURL} -sS "${clickhouse_url_with_session_id}" \
        -H "X-ClickHouse-User: ${username}" -H "X-ClickHouse-Key: ${password}" \
        -d 'SELECT 1 Format Null'
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

# Below are the remaining cases after cutting the profile/role x auth-type cross-product: all
# three profile/role variants only run for `plaintext_password` (it exercises both success and
# failure paths); the other auth types only run the baseline (no profiles, no roles) variant.

function caseXmlUser()
{
    # Special case: user and profile are both defined in XML
    runEndpointTests "User with profile from XML" "no_password" "${XML_USERNAME}" ''
}

function caseNoPassword()
{
    local password="password"
    createUser "no_password" "${NO_PASSWORD_USERNAME}" "${password}"
    runEndpointTests "No profiles no roles" "no_password" "${NO_PASSWORD_USERNAME}" "${RESULTING_PASS}"
}

function casePlaintextBaseline()
{
    local password="password"
    createUser "plaintext_password" "${PLAINTEXT_BASELINE_USERNAME}" "${password}"
    runEndpointTests "No profiles no roles" "plaintext_password" "${PLAINTEXT_BASELINE_USERNAME}" "${RESULTING_PASS}"
}

function casePlaintextProfiles()
{
    local password="password"
    createUser "plaintext_password" "${PLAINTEXT_PROFILES_USERNAME}" "${password}"
    runEndpointTests "Two profiles, no roles" "plaintext_password" "${PLAINTEXT_PROFILES_USERNAME}" "${RESULTING_PASS}" "\
CREATE PROFILE ${PROFILE_NAME_1} SETTINGS max_memory_usage=10000000 TO ${PLAINTEXT_PROFILES_USERNAME};
CREATE PROFILE ${PROFILE_NAME_2} SETTINGS max_rows_to_transfer=1000 TO ${PLAINTEXT_PROFILES_USERNAME};
"
}

function casePlaintextRoles()
{
    local password="password"
    createUser "plaintext_password" "${PLAINTEXT_ROLES_USERNAME}" "${password}"
    runEndpointTests "Two profiles and two simple roles" "plaintext_password" "${PLAINTEXT_ROLES_USERNAME}" "${RESULTING_PASS}" "\
CREATE ROLE ${ROLE_NAME_1};
GRANT ${ROLE_NAME_1} TO ${PLAINTEXT_ROLES_USERNAME};
CREATE ROLE ${ROLE_NAME_2} SETTINGS max_columns_to_read=100;
GRANT ${ROLE_NAME_2} TO ${PLAINTEXT_ROLES_USERNAME};
SET DEFAULT ROLE ${ROLE_NAME_1}, ${ROLE_NAME_2} TO ${PLAINTEXT_ROLES_USERNAME};
"
}

function caseSha256()
{
    local password="password"
    createUser "sha256_password" "${SHA256_USERNAME}" "${password}"
    runEndpointTests "No profiles no roles" "sha256_password" "${SHA256_USERNAME}" "${RESULTING_PASS}"
}

function caseDoubleSha1()
{
    local password="password"
    createUser "double_sha1_password" "${DOUBLE_SHA1_USERNAME}" "${password}"
    runEndpointTests "No profiles no roles" "double_sha1_password" "${DOUBLE_SHA1_USERNAME}" "${RESULTING_PASS}"
}

declare -a CASE_LOG_FILES=()
declare -a CASE_PIDS=()

function runCaseInBackground()
{
    local case_function="${1}"
    local log_file
    log_file=$(mktemp /tmp/tmp_case_output.XXXXXX)
    CASE_LOG_FILES+=("${log_file}")
    "${case_function}" > "${log_file}" 2>&1 &
    CASE_PIDS+=("$!")
}

# to cut off previous runs
start_time="$(executeQuery <<< 'SELECT now64(6);')"
readonly start_time

runCaseInBackground caseXmlUser
runCaseInBackground caseNoPassword
runCaseInBackground casePlaintextBaseline
runCaseInBackground casePlaintextProfiles
runCaseInBackground casePlaintextRoles
runCaseInBackground caseSha256
runCaseInBackground caseDoubleSha1

# Wait for every case regardless of individual failures, so that all of their captured output
# (printed below in a fixed order, matching the order the cases were launched in) is available
# for debugging; fail the test afterwards if any case failed.
declare -a CASE_EXIT_CODES=()
for pid in "${CASE_PIDS[@]}"; do
    exit_code=0
    wait "${pid}" || exit_code=$?
    CASE_EXIT_CODES+=("${exit_code}")
done

for log_file in "${CASE_LOG_FILES[@]}"; do
    cat "${log_file}"
    rm -f "${log_file}"
done

for exit_code in "${CASE_EXIT_CODES[@]}"; do
    if [[ "${exit_code}" -ne 0 ]]
    then
        exit 1
    fi
done

executeQuery <<EOF
SYSTEM FLUSH LOGS session_log;

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
