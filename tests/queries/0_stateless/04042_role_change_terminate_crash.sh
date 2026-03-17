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
# Create role WITHOUT a config-defined profile so the user can establish a session.
${CLICKHOUSE_CLIENT} -q "CREATE ROLE ${P}_role"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${P}_u IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT ${P}_role TO ${P}_u"
${CLICKHOUSE_CLIENT} -q "ALTER USER ${P}_u DEFAULT ROLE ${P}_role"

# Keep an active session alive so ContextAccess holds a subscription for role changes.
# Redirect stderr: when the role later gains a config-defined profile, the session may
# receive ACCESS_DENIED — that is expected and must not fail the test.
${CLICKHOUSE_CLIENT} --user "${P}_u" -q "SELECT sleep(5)" 2>/dev/null &
BG_PID=$!
sleep 0.5

# Add the config-defined profile. This fires RoleCache::roleChanged → scope_guard
# notifications destructor (noexcept) → ContextAccess::setRolesInfo → getEnabledSettings
# → mergeSettingsAndConstraintsFor throws ACCESS_DENIED inside the noexcept destructor
# → std::terminate if the bug is present.
${CLICKHOUSE_CLIENT} -q "ALTER ROLE ${P}_role SETTINGS PROFILE 'readonly'"

wait "${BG_PID}" || true
sleep 0.3

${CLICKHOUSE_CLIENT} --connect_timeout 3 -q "SELECT 'server_alive'"
