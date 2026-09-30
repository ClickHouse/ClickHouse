#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The native client resolves `compatibility` locally, which reverts settings changed in later versions to their
# old defaults. It must not transmit those reverted values as explicit changes: for a setting pinned read-only in
# the profile (explicit value + CONST), that would fail every query even though the server keeps the pinned value.
# The client instead sends `compatibility` itself and lets the server re-derive, so the pin holds. See 04326 for
# the SQL-side (SETTINGS clause / direct SET) coverage; this test covers the native `--compatibility` client path.

setting="s3_allow_server_credentials_in_user_queries"
user="user_${CLICKHOUSE_DATABASE}"
user_compat="user_compat_${CLICKHOUSE_DATABASE}"
profile="profile_${CLICKHOUSE_DATABASE}"
profile_compat="profile_compat_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}, ${user_compat}"
${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE IF EXISTS ${profile}, ${profile_compat}"

${CLICKHOUSE_CLIENT} --query "CREATE SETTINGS PROFILE ${profile} SETTINGS ${setting} = 0 CONST"
# A cloud-tenant-like profile that also pins an old compatibility version alongside the read-only setting.
${CLICKHOUSE_CLIENT} --query "CREATE SETTINGS PROFILE ${profile_compat} SETTINGS compatibility = '25.8', ${setting} = 0 CONST"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} SETTINGS PROFILE '${profile}'"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_compat} SETTINGS PROFILE '${profile_compat}'"

# An old compatibility passed by the client must not fail the query; the pinned value stays in force.
echo -n 'old_compatibility: '
${CLICKHOUSE_CLIENT} --user "${user}" --compatibility 24.1 --query "SELECT getSetting('${setting}')"

# Same when the client's compatibility matches the version the profile pins (the reverted value would otherwise
# be the only setting transmitted, with no `compatibility` in the batch to explain it).
echo -n 'compatibility_matches_profile: '
${CLICKHOUSE_CLIENT} --user "${user_compat}" --compatibility 25.8 --query "SELECT getSetting('${setting}')"

# A genuine attempt to change the read-only setting is still rejected.
echo -n 'explicit_override: '
${CLICKHOUSE_CLIENT} --user "${user}" --"${setting}" 1 --query "SELECT 1" 2>&1 | grep -o -m1 "SETTING_CONSTRAINT_VIOLATION"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}, ${user_compat}"
${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE IF EXISTS ${profile}, ${profile_compat}"
