#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test that config-defined profiles cannot be applied to SQL-defined users
# when `disallow_config_defined_profiles_for_sql_defined_users` is enabled.
# The default profile is always exempt from this restriction.
# Indirect inheritance (SQL profile -> config profile) is also blocked.

user_prefix="${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user_prefix}_u1, ${user_prefix}_u2, ${user_prefix}_u3, ${user_prefix}_u4, ${user_prefix}_u5"
${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE IF EXISTS ${user_prefix}_sql_profile, ${user_prefix}_inheriting_profile"
${CLICKHOUSE_CLIENT} --query "DROP ROLE IF EXISTS ${user_prefix}_role_with_config_profile"

# 1. SQL user with no explicit profile — should connect fine (global default profile is exempt)
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_prefix}_u1 IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --user "${user_prefix}_u1" --query "SELECT 'no_profile_ok'"

# 2. SQL user with config-defined 'default' profile — should succeed (default profile is always exempt)
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_prefix}_u2 IDENTIFIED WITH no_password SETTINGS PROFILE 'default'"
${CLICKHOUSE_CLIENT} --user "${user_prefix}_u2" --query "SELECT 'default_profile_ok'"

# 3. SQL user with non-default config-defined profile 'readonly' — should fail on connect
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_prefix}_u3 IDENTIFIED WITH no_password SETTINGS PROFILE 'readonly'"
${CLICKHOUSE_CLIENT} --user "${user_prefix}_u3" --query "SELECT 'should_not_appear'" 2>&1 | grep -o 'ACCESS_DENIED'

# 4. SQL user with SQL-defined profile — should succeed
${CLICKHOUSE_CLIENT} --query "CREATE SETTINGS PROFILE ${user_prefix}_sql_profile SETTINGS max_threads = 4"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_prefix}_u4 IDENTIFIED WITH no_password SETTINGS PROFILE '${user_prefix}_sql_profile'"
${CLICKHOUSE_CLIENT} --user "${user_prefix}_u4" --query "SELECT 'sql_profile_ok'"

# 5. Role with non-default config-defined profile assigned to SQL user — should fail on connect
${CLICKHOUSE_CLIENT} --query "CREATE ROLE ${user_prefix}_role_with_config_profile SETTINGS PROFILE 'readonly'"
${CLICKHOUSE_CLIENT} --query "GRANT ${user_prefix}_role_with_config_profile TO ${user_prefix}_u1"
${CLICKHOUSE_CLIENT} --query "ALTER USER ${user_prefix}_u1 DEFAULT ROLE ${user_prefix}_role_with_config_profile"
${CLICKHOUSE_CLIENT} --user "${user_prefix}_u1" --query "SELECT 'should_not_appear'" 2>&1 | grep -o 'ACCESS_DENIED'

# 6. SQL profile inheriting from config-defined profile — should fail on connect (indirect inheritance)
${CLICKHOUSE_CLIENT} --query "CREATE SETTINGS PROFILE ${user_prefix}_inheriting_profile SETTINGS PROFILE 'readonly'"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_prefix}_u5 IDENTIFIED WITH no_password SETTINGS PROFILE '${user_prefix}_inheriting_profile'"
${CLICKHOUSE_CLIENT} --user "${user_prefix}_u5" --query "SELECT 'should_not_appear'" 2>&1 | grep -o 'ACCESS_DENIED'

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user_prefix}_u1, ${user_prefix}_u2, ${user_prefix}_u3, ${user_prefix}_u4, ${user_prefix}_u5"
${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE IF EXISTS ${user_prefix}_sql_profile, ${user_prefix}_inheriting_profile"
${CLICKHOUSE_CLIENT} --query "DROP ROLE IF EXISTS ${user_prefix}_role_with_config_profile"
