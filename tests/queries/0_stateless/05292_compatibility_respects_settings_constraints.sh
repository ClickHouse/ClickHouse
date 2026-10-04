#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# `compatibility = '23.6'` sets `function_sleep_max_microseconds_per_block` to 0 (the default is 3000000),
# unless that value is outside what the constraints of the user allow.

user="user_${CLICKHOUSE_DATABASE}"
profile="profile_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -m -q "
    DROP USER IF EXISTS ${user};
    DROP SETTINGS PROFILE IF EXISTS ${profile};
    CREATE USER ${user} IDENTIFIED WITH no_password;
    CREATE SETTINGS PROFILE ${profile} TO ${user};
"

function check()
{
    echo "$1"
    ${CLICKHOUSE_CLIENT} -q "ALTER SETTINGS PROFILE ${profile} SETTINGS $2"
    ${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT getSetting('function_sleep_max_microseconds_per_block') SETTINGS compatibility = '23.6'"
    ${CLICKHOUSE_CLIENT} --user "${user}" -m -q "SET compatibility = '23.6'; SELECT getSetting('function_sleep_max_microseconds_per_block')"
}

check "no constraint" "NONE"
check "range allows the value" "function_sleep_max_microseconds_per_block MIN 0 MAX 5000000"
check "value below minimum" "function_sleep_max_microseconds_per_block MIN 1000"
check "const" "function_sleep_max_microseconds_per_block CONST"
check "changeable_in_readonly with range" "function_sleep_max_microseconds_per_block MIN 1000 CHANGEABLE_IN_READONLY"

echo "compatibility in the profile of the user"
${CLICKHOUSE_CLIENT} -q "ALTER SETTINGS PROFILE ${profile} SETTINGS compatibility = '23.6', function_sleep_max_microseconds_per_block MIN 1000"
${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT getSetting('compatibility'), getSetting('function_sleep_max_microseconds_per_block')"
${CLICKHOUSE_CLIENT} -q "ALTER SETTINGS PROFILE ${profile} SETTINGS compatibility = '23.6', function_sleep_max_microseconds_per_block MIN 0"
${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT getSetting('compatibility'), getSetting('function_sleep_max_microseconds_per_block')"

echo "a value set explicitly is kept"
${CLICKHOUSE_CLIENT} -q "ALTER SETTINGS PROFILE ${profile} SETTINGS function_sleep_max_microseconds_per_block MIN 1000"
${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT getSetting('function_sleep_max_microseconds_per_block') SETTINGS compatibility = '23.6', function_sleep_max_microseconds_per_block = 2000"

${CLICKHOUSE_CLIENT} -m -q "
    DROP USER ${user};
    DROP SETTINGS PROFILE ${profile};
"
