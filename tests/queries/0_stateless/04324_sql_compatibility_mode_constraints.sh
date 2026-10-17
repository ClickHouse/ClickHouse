#!/usr/bin/env bash
# Tags: no-random-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER="u_04324_${CLICKHOUSE_DATABASE}"
PROFILE="p_04324_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
    ${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS ${PROFILE}"
}

trap cleanup EXIT

cleanup
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PROFILE} SETTINGS join_use_nulls = 0 CONST"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER} SETTINGS PROFILE '${PROFILE}'"

${CLICKHOUSE_CLIENT} --user="${USER}" -q "SELECT 1 SETTINGS sql_compatibility_mode = 'standard'" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
${CLICKHOUSE_CLIENT} --user="${USER}" -q "SELECT getSetting('join_use_nulls') SETTINGS sql_compatibility_mode = 'standard', join_use_nulls = 0"
