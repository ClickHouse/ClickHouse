#!/usr/bin/env bash
# Tags: no-random-settings
# Randomized client `max_threads` overrides would replace the readonly profile value being tested.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# Observe the profile's `log_comment`, not the harness's per-test client override.
CLICKHOUSE_LOG_COMMENT=
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

test_prefix="snapshot_${BASHPID}_${RANDOM}"
user_a="${test_prefix}_a"
user_b="${test_prefix}_b"
profile_a="${test_prefix}_profile_a"
profile_b="${test_prefix}_profile_b"
role_name="${test_prefix}_role"
constraint_error=$(mktemp "${CLICKHOUSE_TMP}/${test_prefix}_constraint.XXXXXX")

cleanup()
{
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        DROP USER IF EXISTS ${user_a}, ${user_b};
        DROP SETTINGS PROFILE IF EXISTS ${profile_a}, ${profile_b};
        DROP ROLE IF EXISTS ${role_name};
    "
    rm -- "$constraint_error"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE ROLE ${role_name};
    CREATE USER ${user_a};
    CREATE USER ${user_b};
    GRANT ${role_name} TO ${user_a};
    CREATE SETTINGS PROFILE ${profile_a}
        SETTINGS max_threads = 5 READONLY, log_comment = 'profile-a'
        TO ${role_name};
    CREATE SETTINGS PROFILE ${profile_b}
        SETTINGS max_threads = 7 READONLY, log_comment = 'profile-b'
        TO ${user_b};
"

read_profile()
{
    ${CLICKHOUSE_CLIENT} --user "$1" --query "SELECT getSetting('max_threads'), getSetting('log_comment')"
}

# Separate connections exercise login-cache hits without a persistent session.
read_profile "$user_a"
read_profile "$user_a"
read_profile "$user_b"
read_profile "$user_b"

# A query-local override must not mutate the cached user/role configuration.
${CLICKHOUSE_CLIENT} --user "$user_a" --query "SELECT getSetting('log_comment') SETTINGS log_comment = 'query-local'"
read_profile "$user_a"

# Warm-cache logins retain profile constraints.
if ${CLICKHOUSE_CLIENT} --user "$user_a" --query "SELECT 1 SETTINGS max_threads = 3" > "$constraint_error" 2>&1; then
    echo 'Expected the readonly profile constraint to reject max_threads'
    exit 1
fi
grep -Eq 'SETTING_CONSTRAINT_VIOLATION|READONLY' "$constraint_error"
echo 'constraint enforced'

# Replacing a profile generation invalidates its resolved snapshot.
${CLICKHOUSE_CLIENT} --query "ALTER SETTINGS PROFILE ${profile_a} SETTINGS max_threads = 6 READONLY, log_comment = 'profile-a-v2'"
read_profile "$user_a"
read_profile "$user_a"
read_profile "$user_b"

# A changed role/profile assignment must not reuse the earlier user's resolved values.
${CLICKHOUSE_CLIENT} --multiquery --query "
    ALTER SETTINGS PROFILE ${profile_b} TO ${user_a}, ${user_b};
    REVOKE ${role_name} FROM ${user_a};
"
read_profile "$user_a"
read_profile "$user_b"
