#!/usr/bin/env bash

# `SET <name> = DEFAULT` restores the default that is in effect for the setting, which under an active
# `compatibility` is the value of that version rather than the declared default.
#
# Values are read back over HTTP with a persistent session and a bare URL: a client session resends the
# settings it believes are changed, which re-applies `compatibility` and hides the reset.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# All changed in 26.8. `P`: declared default 0, under 26.7 1. `Q`: declared default 1, under 26.7 0.
# `R`: a `Settings` setting despite the prefix, declared default 65536, under 26.7 0.
P=input_format_read_datetime_number_as_raw_value
Q=enable_group_by_top_k_optimization
R=merge_tree_min_bytes_per_read_stream

USER_MIN="u_min_05047_${CLICKHOUSE_DATABASE}"
USER_RO="u_ro_05047_${CLICKHOUSE_DATABASE}"
USER_P="u_p_05047_${CLICKHOUSE_DATABASE}"
PROFILE_MIN="p_min_05047_${CLICKHOUSE_DATABASE}"
PROFILE_RO="p_ro_05047_${CLICKHOUSE_DATABASE}"
PROFILE_P="p_p_05047_${CLICKHOUSE_DATABASE}"

BASE_URL="${CLICKHOUSE_URL%%\?*}"
session_url() { echo "${BASE_URL}?session_id=s_05047_${CLICKHOUSE_DATABASE}_$$_$1${2:+&user=$2}"; }
read_setting() { ${CLICKHOUSE_CURL} -sS "$1" -d "SELECT value FROM system.settings WHERE name = '$2'"; }

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER_MIN}, ${USER_RO}, ${USER_P}"
${CLICKHOUSE_CLIENT} -q "DROP PROFILE IF EXISTS ${PROFILE_MIN}, ${PROFILE_RO}, ${PROFILE_P}"
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PROFILE_MIN} SETTINGS ${Q} = 1 MIN 1"
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PROFILE_RO} SETTINGS compatibility = '26.7', ${P} = 0, ${R} = 65536, readonly = 1"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER_MIN} SETTINGS PROFILE '${PROFILE_MIN}'"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER_RO} SETTINGS PROFILE '${PROFILE_RO}'"
# Allows the 26.7 value of `P` and forbids its declared default.
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PROFILE_P} SETTINGS ${P} = 1 MIN 1"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER_P} SETTINGS PROFILE '${PROFILE_P}'"

echo 'the probes differ from their declared defaults under compatibility 26.7'
U=$(session_url a0)
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7'"
${CLICKHOUSE_CURL} -sS "$U" -d "SELECT name, value != default FROM system.settings WHERE name IN ('${P}', '${Q}', '${R}') ORDER BY name"

echo 'SET name = DEFAULT'
U=$(session_url a1)
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7'"
${CLICKHOUSE_CURL} -sS "$U" -d "SET ${P} = 0"
${CLICKHOUSE_CURL} -sS "$U" -d "SET ${P} = DEFAULT"
read_setting "$U" "${P}"
# The reset leaves the setting to `compatibility`, so clearing `compatibility` moves it too.
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = DEFAULT"
read_setting "$U" "${P}"

echo 'SET compatibility and the reset in one statement'
U=$(session_url a2)
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7', ${P} = DEFAULT"
read_setting "$U" "${P}"

echo 'SETTINGS name = DEFAULT in a query'
U=$(session_url a3)
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7'"
${CLICKHOUSE_CURL} -sS "$U" -d "SET ${P} = 0"
${CLICKHOUSE_CURL} -sS "$U" -d "SELECT value FROM system.settings WHERE name = '${P}' SETTINGS ${P} = DEFAULT"

echo 'SET compatibility = DEFAULT reverts what it derived'
U=$(session_url a4)
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7'"
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = DEFAULT"
read_setting "$U" "${P}"

echo 'a reset does not take a value from compatibility that the constraints forbid'
U=$(session_url a5 "${USER_MIN}")
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7'"
${CLICKHOUSE_CURL} -sS "$U" -d "SET ${Q} = DEFAULT"
read_setting "$U" "${Q}"

echo 'a reset that would change a setting is refused in readonly mode'
U=$(session_url a6 "${USER_RO}")
${CLICKHOUSE_CURL} -sS "$U" -d "SET ${P} = DEFAULT" | grep -o 'Code: 164' | head -1
read_setting "$U" "${P}"

echo 'a merge_tree_-prefixed name that Settings owns is checked against the value it lands on'
U=$(session_url a7 "${USER_RO}")
${CLICKHOUSE_CURL} -sS "$U" -d "SET ${R} = DEFAULT" | grep -o 'Code: 164' | head -1
read_setting "$U" "${R}"

echo 'a reset is refused when the declared default is forbidden, as compatibility can move the setting back to it'
U=$(session_url a8 "${USER_P}")
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7'"
${CLICKHOUSE_CURL} -sS "$U" -d "SET ${P} = DEFAULT" | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
read_setting "$U" "${P}"

echo 'resetting compatibility keeps what make_distributed_plan adjusts'
# `compile_expressions` is 0 under 25.4 and declared 1, and `make_distributed_plan` requires 0. A server
# query applies its own settings and adjusts them again, so read it where nothing does.
${CLICKHOUSE_LOCAL} -q "SET compatibility = '25.4'; SET make_distributed_plan = 1; SET compatibility = DEFAULT; SELECT value FROM system.settings WHERE name = 'compile_expressions'"

${CLICKHOUSE_CLIENT} -q "DROP USER ${USER_MIN}, ${USER_RO}, ${USER_P}"
${CLICKHOUSE_CLIENT} -q "DROP PROFILE ${PROFILE_MIN}, ${PROFILE_RO}, ${PROFILE_P}"
