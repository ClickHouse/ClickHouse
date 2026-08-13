#!/usr/bin/env bash
# Tags: no-old-analyzer
# no-old-analyzer: make_distributed_plan requires the analyzer.

# The constraints of the user's own profile arrive at login, after the profile's settings are
# applied. A `const` constraint there must still veto the make_distributed_plan derivation
# from the profile's distributed_plan_workers_num. The environment itself may already pin the
# setting const for every user (ClickHouse Cloud does); the probe below reads the ambient pin
# from a readonly=0 session, and the expectations follow it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FREE_USER="u_free_${CLICKHOUSE_TEST_UNIQUE_NAME}"
FREE_PROFILE="p_free_${CLICKHOUSE_TEST_UNIQUE_NAME}"
PINNED_USER="u_pinned_${CLICKHOUSE_TEST_UNIQUE_NAME}"
PINNED_PROFILE="p_pinned_${CLICKHOUSE_TEST_UNIQUE_NAME}"
READONLY_USER="u_readonly_${CLICKHOUSE_TEST_UNIQUE_NAME}"
READONLY_PROFILE="p_readonly_${CLICKHOUSE_TEST_UNIQUE_NAME}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${FREE_USER}, ${PINNED_USER}, ${READONLY_USER}"
    ${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS ${FREE_PROFILE}, ${PINNED_PROFILE}, ${READONLY_PROFILE}"
}
trap cleanup EXIT
cleanup

AMBIENT_PIN=$(${CLICKHOUSE_CLIENT} -q "SELECT readonly FROM system.settings WHERE name = 'make_distributed_plan'")
if [ "${AMBIENT_PIN}" == "0" ]; then EXPECTED_DERIVED="true"; else EXPECTED_DERIVED="false"; fi

${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${FREE_PROFILE} SETTINGS distributed_plan_workers_num = 3"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${FREE_USER} SETTINGS PROFILE ${FREE_PROFILE}"

${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PINNED_PROFILE} SETTINGS distributed_plan_workers_num = 3, make_distributed_plan CONST"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${PINNED_USER} SETTINGS PROFILE ${PINNED_PROFILE}"

${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${READONLY_PROFILE} SETTINGS readonly = 1, distributed_plan_workers_num = 3"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${READONLY_USER} SETTINGS PROFILE ${READONLY_PROFILE}"

check()
{
    local label=$1 user=$2 expected=$3
    local actual
    # The bare binary, not ${CLICKHOUSE_CLIENT}: the harness options it carries (send_logs_level,
    # log_comment, randomized settings) are rejected at query start by a readonly=1 session.
    actual=$(${CLICKHOUSE_CLIENT_BINARY} --host "${CLICKHOUSE_HOST}" --port "${CLICKHOUSE_PORT_TCP}" --user "${user}" -q "SELECT getSetting('make_distributed_plan')")
    echo "${label}"
    if [ "${actual}" == "${expected}" ]; then echo "OK"; else echo "FAIL: got ${actual}, expected ${expected}"; fi
}

check "derived at login without the constraint" "${FREE_USER}" "${EXPECTED_DERIVED}"
check "vetoed at login by the const constraint" "${PINNED_USER}" "false"
check "still derived in a readonly session" "${READONLY_USER}" "${EXPECTED_DERIVED}"
