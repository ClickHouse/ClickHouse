#!/usr/bin/env bash
# Verify that `s3_allow_server_credentials_in_user_queries` can be locked to its secure value for an
# untrusted profile so that neither a direct change nor an old `compatibility` version can re-enable it.
# A `readonly` constraint alone is not enough: `compatibility` with a version before this setting was
# introduced would restore the old (allowing) default. Pinning the value explicitly with `CONST` (the
# SQL form of "explicit value + readonly") defeats `compatibility`.
#
# The user and profile names are qualified with $CLICKHOUSE_DATABASE so the test can run in parallel.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER="u_04326_${CLICKHOUSE_DATABASE}"
PROFILE="p_04326_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP PROFILE IF EXISTS ${PROFILE}"

# Mimic a cloud tenant profile: an old pinned compatibility version (set when the service was created)
# together with the explicit-value + CONST lock on our setting.
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PROFILE} SETTINGS compatibility = '25.8', s3_allow_server_credentials_in_user_queries = 0 CONST"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER} SETTINGS PROFILE '${PROFILE}'"

# The pinned value holds despite the old compatibility version in the same profile.
echo -n 'compat_pinned: '
${CLICKHOUSE_CLIENT} --user="${USER}" -q "SELECT getSetting('s3_allow_server_credentials_in_user_queries')"

# A direct attempt to enable it is rejected.
echo -n 'direct_set: '
${CLICKHOUSE_CLIENT} --user="${USER}" -q "SELECT 1 SETTINGS s3_allow_server_credentials_in_user_queries = 1" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1

# Trying to enable it via an old compatibility version passed in the session is also rejected.
echo -n 'compat_set: '
${CLICKHOUSE_CLIENT} --user="${USER}" -q "SELECT 1 SETTINGS compatibility = '25.8', s3_allow_server_credentials_in_user_queries = 1" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1

${CLICKHOUSE_CLIENT} -q "DROP USER ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP PROFILE ${PROFILE}"
