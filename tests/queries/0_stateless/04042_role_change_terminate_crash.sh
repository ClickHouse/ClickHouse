#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

P="${CLICKHOUSE_TEST_UNIQUE_NAME}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${P}_u"    2>/dev/null || true
    ${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS ${P}_role" 2>/dev/null || true
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS ${P}_role"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${P}_u"
${CLICKHOUSE_CLIENT} -q "CREATE ROLE ${P}_role SETTINGS PROFILE 'readonly'"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${P}_u IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT ${P}_role TO ${P}_u"
${CLICKHOUSE_CLIENT} -q "ALTER USER ${P}_u DEFAULT ROLE ${P}_role"

# Keep an active session alive so ContextAccess holds a subscription for role changes.
${CLICKHOUSE_CLIENT} --user "${P}_u" -q "SELECT sleep(5)" &
BG_PID=$!
sleep 0.5

# Modifying the role triggers RoleCache::roleChanged, which fires ContextAccess::setRolesInfo
# via the scope_guard `notifications` destructor. That destructor is noexcept; if
# mergeSettingsAndConstraintsFor throws ACCESS_DENIED (unprotected in getEnabledSettings),
# the server crashes via std::terminate.
${CLICKHOUSE_CLIENT} -q "ALTER ROLE ${P}_role SETTINGS max_threads = 2"

wait "${BG_PID}" || true
sleep 0.3

${CLICKHOUSE_CLIENT} --connect_timeout 3 -q "SELECT 'server_alive'"
