#!/usr/bin/env bash

# `SET <name> = DEFAULT` restores the default that is in effect for the setting, which under an active
# `compatibility` is the value that version implies rather than the current version's declared default.
#
# Every value is read back over HTTP with a persistent session and a bare URL. A plain client session
# mirrors the settings it believes are changed and resends them per query, so `compatibility` is
# re-applied on the next statement and papers the reset over; the bare URL also keeps the test runner's
# randomized settings out of the session.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Both probes changed in 26.8 and are never randomized by the test runner.
# `input_format_read_datetime_number_as_raw_value`: declared default false, under `26.7` true.
# `enable_group_by_top_k_optimization`: declared default true, under `26.7` false - the polarity a
# `MIN 1` constraint needs.
P=input_format_read_datetime_number_as_raw_value
Q=enable_group_by_top_k_optimization

USER_MIN="u_min_05047_${CLICKHOUSE_DATABASE}"
USER_CONST="u_const_05047_${CLICKHOUSE_DATABASE}"
USER_PROF="u_prof_05047_${CLICKHOUSE_DATABASE}"
PROFILE_MIN="p_min_05047_${CLICKHOUSE_DATABASE}"
PROFILE_CONST="p_const_05047_${CLICKHOUSE_DATABASE}"
PROFILE_COMPAT="p_compat_05047_${CLICKHOUSE_DATABASE}"

BASE_URL="${CLICKHOUSE_URL%%\?*}"
session_url() { echo "${BASE_URL}?session_id=s_05047_${CLICKHOUSE_DATABASE}_$$_$1"; }
user_session_url() { echo "${BASE_URL}?session_id=s_05047_${CLICKHOUSE_DATABASE}_$$_$1&user=$2"; }
# `system.settings` is read at execution time, so it also reports a reset made by the same statement.
read_setting() { ${CLICKHOUSE_CURL} -sS "$1" -d "SELECT value FROM system.settings WHERE name = '$2'"; }

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER_MIN}, ${USER_CONST}, ${USER_PROF}"
${CLICKHOUSE_CLIENT} -q "DROP PROFILE IF EXISTS ${PROFILE_MIN}, ${PROFILE_CONST}, ${PROFILE_COMPAT}"
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PROFILE_MIN} SETTINGS ${Q} = 1 MIN 1"
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PROFILE_CONST} SETTINGS compatibility = '26.7' CONST"
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PROFILE_COMPAT} SETTINGS compatibility = '26.7'"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER_MIN} SETTINGS PROFILE '${PROFILE_MIN}'"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER_CONST} SETTINGS PROFILE '${PROFILE_CONST}'"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER_PROF} SETTINGS PROFILE '${PROFILE_MIN}'"

echo 'the probe values differ from their declared defaults under compatibility 26.7'
# If either 26.8 history row is ever dropped, this fails loudly instead of leaving the arms below vacuous.
U=$(session_url a0)
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7'"
${CLICKHOUSE_CURL} -sS "$U" -d "SELECT name, value != default FROM system.settings WHERE name IN ('${P}', '${Q}') ORDER BY name"

echo 'SET name = DEFAULT'
U=$(session_url a1)
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7'"
${CLICKHOUSE_CURL} -sS "$U" -d "SET ${P} = DEFAULT"
read_setting "$U" "${P}"

echo 'SET name = DEFAULT after the setting was assigned explicitly'
U=$(session_url a2)
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7'"
${CLICKHOUSE_CURL} -sS "$U" -d "SET ${P} = 0"
${CLICKHOUSE_CURL} -sS "$U" -d "SET ${P} = DEFAULT"
read_setting "$U" "${P}"

echo 'SET compatibility = DEFAULT reverts what it derived'
U=$(session_url a3)
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7'"
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = DEFAULT"
read_setting "$U" "${P}"
read_setting "$U" "${Q}"

echo 'an explicitly assigned setting is left alone by a later compatibility change'
U=$(session_url a4)
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7'"
${CLICKHOUSE_CURL} -sS "$U" -d "SET ${P} = 0"
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.6'"
read_setting "$U" "${P}"

echo 'SETTINGS name = DEFAULT in a query'
# A different dispatch hop than the standalone statement above. `P` is assigned first, so the
# query-local reset has to move it while the session keeps the assigned value.
U=$(session_url a5)
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7'"
${CLICKHOUSE_CURL} -sS "$U" -d "SET ${P} = 0"
${CLICKHOUSE_CURL} -sS "$U" -d "SELECT value FROM system.settings WHERE name = '${P}' SETTINGS ${P} = DEFAULT"
read_setting "$U" "${P}"

echo 'a reset that lands on a value the profile forbids is rejected'
# The profile assignment keeps the setting changed, so `compatibility` leaves it alone and the reset is
# the only way to reach the era value.
U=$(user_session_url a6 "${USER_MIN}")
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7'"
${CLICKHOUSE_CURL} -sS "$U" -d "SET ${Q} = DEFAULT" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
read_setting "$U" "${Q}"

echo 'and when the same statement is what activates compatibility'
U=$(user_session_url a7 "${USER_MIN}")
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7', ${Q} = DEFAULT" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
# The whole statement is left without effect, so compatibility is not set either.
read_setting "$U" "compatibility"
read_setting "$U" "${Q}"

echo 'and when the same statement is what switches the profile'
U=$(user_session_url a8 "${USER_PROF}")
${CLICKHOUSE_CURL} -sS "$U" -d "SET profile = '${PROFILE_COMPAT}', ${Q} = DEFAULT" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
read_setting "$U" "${Q}"

echo 'resetting a CONST compatibility is still rejected'
U=$(user_session_url a9 "${USER_CONST}")
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = DEFAULT" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
read_setting "$U" "compatibility"
read_setting "$U" "${P}"

echo 'SET compatibility = DEFAULT re-applies the settings post-processors'
# `compile_expressions` is forced off while `make_distributed_plan` is on, and its 25.5 history row
# makes it compatibility-derivable, so reverting the derivation must not resurrect it. The two reads
# before the reset arm the arm: the setting is derived off, and the adjustment has a reason to act.
U=$(session_url a10)
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '25.4'"
read_setting "$U" compile_expressions
${CLICKHOUSE_CURL} -sS "$U" -d "SET make_distributed_plan = 1"
read_setting "$U" make_distributed_plan
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = DEFAULT"
# Each query re-runs the adjustment on its own context, so the session value is only legible once
# `make_distributed_plan` is off again, which by itself never moves `compile_expressions`.
${CLICKHOUSE_CURL} -sS "$U" -d "SET make_distributed_plan = 0"
read_setting "$U" compile_expressions

echo 'and the spelling it has to agree with reaches the same state'
U=$(session_url a11)
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '25.4'"
${CLICKHOUSE_CURL} -sS "$U" -d "SET make_distributed_plan = 1"
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = ''"
${CLICKHOUSE_CURL} -sS "$U" -d "SET make_distributed_plan = 0"
read_setting "$U" compile_expressions

${CLICKHOUSE_CLIENT} -q "DROP USER ${USER_MIN}, ${USER_CONST}, ${USER_PROF}"
${CLICKHOUSE_CLIENT} -q "DROP PROFILE ${PROFILE_MIN}, ${PROFILE_CONST}, ${PROFILE_COMPAT}"
