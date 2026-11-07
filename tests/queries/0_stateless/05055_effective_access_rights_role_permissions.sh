#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

user="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"
role_a="ra_${CLICKHOUSE_TEST_UNIQUE_NAME}"
role_b="rb_${CLICKHOUSE_TEST_UNIQUE_NAME}"
db="db_${CLICKHOUSE_TEST_UNIQUE_NAME}"
base_url="${CLICKHOUSE_URL%%\?*}"

function admin()
{
    ${CLICKHOUSE_CURL} -fsS "${CLICKHOUSE_URL}" --data-binary "$1"
}

function cleanup()
{
    admin "DROP USER IF EXISTS ${user}"
    admin "DROP ROLE IF EXISTS ${role_a}, ${role_b}"
    admin "DROP DATABASE IF EXISTS ${db}"
}
trap cleanup EXIT

cleanup
admin "CREATE DATABASE ${db}"
admin "CREATE TABLE ${db}.seed (x UInt8) ENGINE = Memory"
admin "CREATE TABLE ${db}.target (x UInt8) ENGINE = Memory"
admin "CREATE ROLE ${role_a}, ${role_b}"
admin "GRANT SELECT ON ${db}.seed TO ${role_a}, ${role_b}"
admin "CREATE USER ${user} IDENTIFIED WITH no_password"
admin "GRANT ${role_a}, ${role_b} TO ${user}"
user_before=$(admin "SHOW CREATE USER ${user}")

# Keep four HTTP named sessions alive. The first two use the same role with
# different databases, exercising shared results across distinct access contexts.
session_urls=()
databases=("${CLICKHOUSE_DATABASE}" "${db}" "${db}" "${db}")
roles=("${role_a}" "${role_a}" "${role_b}" "${role_a}, ${role_b}")
for i in 0 1 2 3; do
    url="${base_url}?user=${user}&database=${databases[$i]}&session_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_${i}&session_timeout=60"
    ${CLICKHOUSE_CURL} -fsS "${url}" --data-binary "SET ROLE ${roles[$i]}"
    session_urls+=("${url}&session_check=1")
done

function check_sessions()
{
    local stage=$1
    local expected=$2
    local result=""
    local answer
    for i in 0 1 2 3; do
        answer=$(${CLICKHOUSE_CURL} -fsS "${session_urls[$i]}" --data-binary "CHECK GRANT SELECT ON ${db}.target")
        test "${answer}" = "${expected:$i:1}"
        result+="${answer}"
        answer=$(${CLICKHOUSE_CURL} -fsS "${session_urls[$i]}" --data-binary "CHECK GRANT INSERT ON ${db}.target")
        test "${answer}" = "0"

        if [[ "${expected:$i:1}" = "1" ]]; then
            answer=$(${CLICKHOUSE_CURL} -fsS "${session_urls[$i]}" --data-binary "SELECT count() FROM ${db}.target")
            test "${answer}" = "0"
        else
            answer=$(${CLICKHOUSE_CURL} -sS "${session_urls[$i]}" --data-binary "SELECT count() FROM ${db}.target")
            [[ "${answer}" = *ACCESS_DENIED* ]]
        fi
    done
    test "$(admin "SHOW CREATE USER ${user}")" = "${user_before}"
    echo "${stage}: ${result}"
}

# Warm the old sessions before changing the roles themselves. The user's role
# membership stays fixed, so invalidation depends on new `EnabledRolesInfo` snapshots.
check_sessions initial 0000
admin "GRANT SELECT ON ${db}.target TO ${role_a}"
check_sessions grant_a 1101
admin "REVOKE SELECT ON ${db}.target FROM ${role_a}"
check_sessions revoke_a 0000
admin "GRANT SELECT ON ${db}.target TO ${role_b}"
check_sessions grant_b 0011
admin "REVOKE SELECT ON ${db}.target FROM ${role_b}"
check_sessions revoke_b 0000

# Changing the user keeps the role snapshots unchanged. Exercise reuse of the
# same cache slots for a new `User` snapshot in the established sessions.
admin "GRANT SELECT ON ${db}.target TO ${user}"
check_sessions user_grant 1111
admin "REVOKE SELECT ON ${db}.target FROM ${user}"
check_sessions user_revoke 0000
