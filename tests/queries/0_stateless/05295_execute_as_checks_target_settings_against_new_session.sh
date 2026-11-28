#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# The caller's profile constrains `function_sleep_max_microseconds_per_block` to at most 1000, while the
# target user stores 2000, which a new session of the target accepts. `EXECUTE AS` checks the target's
# settings the way its own login does, not against the caller's constraints.

caller="caller_${CLICKHOUSE_DATABASE}"
target="target_${CLICKHOUSE_DATABASE}"
profile="profile_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -m -q "
    DROP USER IF EXISTS ${caller}, ${target};
    DROP SETTINGS PROFILE IF EXISTS ${profile};
    CREATE USER ${caller} IDENTIFIED WITH no_password;
    CREATE USER ${target} IDENTIFIED WITH no_password SETTINGS function_sleep_max_microseconds_per_block = 2000;
    CREATE SETTINGS PROFILE ${profile} SETTINGS function_sleep_max_microseconds_per_block MAX 1000 TO ${caller};
    GRANT IMPERSONATE ON ${target} TO ${caller};
"

${CLICKHOUSE_CLIENT} --user "${caller}" -q "EXECUTE AS ${target} SELECT currentUser() = '${target}', getSetting('function_sleep_max_microseconds_per_block')"
${CLICKHOUSE_CLIENT} --user "${caller}" -m -q "EXECUTE AS ${target}; SELECT currentUser() = '${target}', getSetting('function_sleep_max_microseconds_per_block')"

${CLICKHOUSE_CLIENT} -m -q "
    DROP USER ${caller}, ${target};
    DROP SETTINGS PROFILE ${profile};
"
