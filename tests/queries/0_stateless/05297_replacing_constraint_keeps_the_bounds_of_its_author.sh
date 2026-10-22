#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# A constraint replaces the one it overrides as a whole, so a user defined by SQL whose profile bounds
# `function_sleep_max_microseconds_per_block` to [1000, 5000] cannot write a constraint on that setting
# which leaves either bound out: whoever gets it could then go below 1000 or above 5000.

admin="admin_${CLICKHOUSE_DATABASE}"
admin_profile="admin_profile_${CLICKHOUSE_DATABASE}"
profile="profile_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -m -q "
    DROP USER IF EXISTS ${admin};
    DROP SETTINGS PROFILE IF EXISTS ${admin_profile}, ${profile};
    CREATE USER ${admin} IDENTIFIED WITH no_password;
    GRANT ACCESS MANAGEMENT ON *.* TO ${admin} WITH GRANT OPTION;
    CREATE SETTINGS PROFILE ${admin_profile} SETTINGS function_sleep_max_microseconds_per_block MIN 1000 MAX 5000 TO ${admin};
"

function as_admin()
{
    echo -n "$1: "
    ${CLICKHOUSE_CLIENT} --user "${admin}" -q "CREATE SETTINGS PROFILE OR REPLACE ${profile} SETTINGS $1" 2>&1 \
        | grep -q -F "SETTING_CONSTRAINT_VIOLATION" && echo "refused" || echo "ok"
}

as_admin "function_sleep_max_microseconds_per_block MAX 4000"
as_admin "function_sleep_max_microseconds_per_block MIN 2000"
as_admin "function_sleep_max_microseconds_per_block CHANGEABLE_IN_READONLY"
as_admin "function_sleep_max_microseconds_per_block MIN 2000 MAX 4000"
as_admin "function_sleep_max_microseconds_per_block CONST"
as_admin "function_sleep_max_microseconds_per_block = 3000"

echo -n "config-defined user, MAX 4000: "
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE OR REPLACE ${profile} SETTINGS function_sleep_max_microseconds_per_block MAX 4000" && echo "ok"

${CLICKHOUSE_CLIENT} -m -q "
    DROP USER ${admin};
    DROP SETTINGS PROFILE ${admin_profile}, ${profile};
"
