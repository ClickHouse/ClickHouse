#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# A user defined by SQL whose profile constrains `function_sleep_max_microseconds_per_block` cannot remove
# that setting from a user, in any form of `ALTER` or by replacing the user, because that would weaken
# the constraint. Changes that keep the setting are allowed, and so is anything by a config-defined user.

admin="admin_${CLICKHOUSE_DATABASE}"
target="target_${CLICKHOUSE_DATABASE}"
admin_profile="admin_profile_${CLICKHOUSE_DATABASE}"
target_profile="target_profile_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -m -q "
    DROP USER IF EXISTS ${admin}, ${target};
    DROP SETTINGS PROFILE IF EXISTS ${admin_profile}, ${target_profile};
    CREATE USER ${admin} IDENTIFIED WITH no_password;
    GRANT ACCESS MANAGEMENT ON *.* TO ${admin} WITH GRANT OPTION;
    CREATE SETTINGS PROFILE ${admin_profile} SETTINGS function_sleep_max_microseconds_per_block MIN 1000 TO ${admin};
    CREATE SETTINGS PROFILE ${target_profile} SETTINGS function_sleep_max_microseconds_per_block = 2000;
"

# `$1` is the definition of the target user, `$2` the statement the admin runs on it.
function as_admin()
{
    ${CLICKHOUSE_CLIENT} -q "CREATE USER OR REPLACE ${target} IDENTIFIED WITH no_password $1"
    echo -n "${2//${CLICKHOUSE_DATABASE}/db}: "
    ${CLICKHOUSE_CLIENT} --user "${admin}" -q "$2" 2>&1 | grep -q -F "SETTING_CONSTRAINT_VIOLATION" && echo "refused" || echo "ok"
}

direct="SETTINGS function_sleep_max_microseconds_per_block = 2000, log_comment = 'a'"
as_admin "${direct}" "ALTER USER ${target} DROP SETTINGS function_sleep_max_microseconds_per_block"
as_admin "${direct}" "ALTER USER ${target} DROP ALL SETTINGS"
as_admin "${direct}" "ALTER USER ${target} SETTINGS log_comment = 'b'"
as_admin "${direct}" "CREATE USER OR REPLACE ${target} IDENTIFIED WITH no_password"
as_admin "${direct}" "ALTER USER ${target} MODIFY SETTINGS function_sleep_max_microseconds_per_block = 3000"
as_admin "${direct}" "ALTER USER ${target} DROP SETTINGS log_comment"
as_admin "${direct}" "ALTER USER ${target} SETTINGS function_sleep_max_microseconds_per_block = 3000"

inherited="SETTINGS PROFILE '${target_profile}'"
as_admin "${inherited}" "ALTER USER ${target} DROP ALL PROFILES"
as_admin "${inherited}" "ALTER USER ${target} DROP PROFILES '${target_profile}'"
# The setting is still inherited from the profile, so dropping the user's own value removes nothing.
as_admin "${inherited}, function_sleep_max_microseconds_per_block = 3000" "ALTER USER ${target} DROP ALL SETTINGS"

echo -n "config-defined user, DROP ALL SETTINGS: "
${CLICKHOUSE_CLIENT} -q "CREATE USER OR REPLACE ${target} IDENTIFIED WITH no_password ${direct}"
${CLICKHOUSE_CLIENT} -q "ALTER USER ${target} DROP ALL SETTINGS" && echo "ok"

${CLICKHOUSE_CLIENT} -m -q "
    DROP USER ${admin}, ${target};
    DROP SETTINGS PROFILE ${admin_profile}, ${target_profile};
"
