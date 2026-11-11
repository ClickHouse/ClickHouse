#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"
PROFILE="p_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -n -q "
    DROP SETTINGS PROFILE IF EXISTS ${PROFILE};
    DROP USER IF EXISTS ${USER};
    CREATE USER ${USER};
    CREATE SETTINGS PROFILE ${PROFILE};
    ALTER USER ${USER} SETTINGS PROFILE ${PROFILE};
"

${CLICKHOUSE_CLIENT} -q "SELECT * FROM system.settings_profile_elements WHERE user_name='${USER}' OR profile_name='${PROFILE}'" | sed "s/${USER}/test_01605/g; s/${PROFILE}/test_01605/g"

${CLICKHOUSE_CLIENT} -n -q "
    DROP SETTINGS PROFILE ${PROFILE};
    SELECT 'PROFILE DROPPED';
"

${CLICKHOUSE_CLIENT} -q "SELECT * FROM system.settings_profile_elements WHERE user_name='${USER}' OR profile_name='${PROFILE}'" | sed "s/${USER}/test_01605/g; s/${PROFILE}/test_01605/g"

${CLICKHOUSE_CLIENT} -n -q "
    DROP USER IF EXISTS ${USER};
    DROP SETTINGS PROFILE IF EXISTS ${PROFILE};
"
