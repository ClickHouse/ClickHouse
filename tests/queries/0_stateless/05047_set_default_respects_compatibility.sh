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
# `merge_tree_min_bytes_per_read_stream`: a `Settings` setting despite the prefix, declared default
# 65536, under `26.7` zero.
P=input_format_read_datetime_number_as_raw_value
Q=enable_group_by_top_k_optimization
R=merge_tree_min_bytes_per_read_stream

USER_MIN="u_min_05047_${CLICKHOUSE_DATABASE}"
USER_CONST="u_const_05047_${CLICKHOUSE_DATABASE}"
USER_STREAM="u_stream_05047_${CLICKHOUSE_DATABASE}"
USER_LOGIN="u_login_05047_${CLICKHOUSE_DATABASE}"
USER_MT="u_mt_05047_${CLICKHOUSE_DATABASE}"
PROFILE_MIN="p_min_05047_${CLICKHOUSE_DATABASE}"
PROFILE_CONST="p_const_05047_${CLICKHOUSE_DATABASE}"
PROFILE_STREAM="p_stream_05047_${CLICKHOUSE_DATABASE}"
PROFILE_LOGIN="p_login_05047_${CLICKHOUSE_DATABASE}"
PROFILE_MT="p_mt_05047_${CLICKHOUSE_DATABASE}"

BASE_URL="${CLICKHOUSE_URL%%\?*}"
session_url() { echo "${BASE_URL}?session_id=s_05047_${CLICKHOUSE_DATABASE}_$$_$1"; }
user_session_url() { echo "${BASE_URL}?session_id=s_05047_${CLICKHOUSE_DATABASE}_$$_$1&user=$2"; }
# `system.settings` is read at execution time, so it also reports a reset made by the same statement.
read_setting() { ${CLICKHOUSE_CURL} -sS "$1" -d "SELECT value FROM system.settings WHERE name = '$2'"; }

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t1_05047, ${CLICKHOUSE_DATABASE}.t2_05047"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER_MIN}, ${USER_CONST}, ${USER_STREAM}, ${USER_LOGIN}, ${USER_MT}"
${CLICKHOUSE_CLIENT} -q "DROP PROFILE IF EXISTS ${PROFILE_MIN}, ${PROFILE_CONST}, ${PROFILE_STREAM}, ${PROFILE_LOGIN}, ${PROFILE_MT}"
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PROFILE_MIN} SETTINGS ${Q} = 1 MIN 1"
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PROFILE_CONST} SETTINGS compatibility = '26.7' CONST"
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PROFILE_STREAM} SETTINGS ${R} MIN 1"
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PROFILE_LOGIN} SETTINGS compatibility = '26.7', ${Q} MIN 1"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER_MIN} SETTINGS PROFILE '${PROFILE_MIN}'"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER_CONST} SETTINGS PROFILE '${PROFILE_CONST}'"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER_STREAM} SETTINGS PROFILE '${PROFILE_STREAM}'"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER_LOGIN} SETTINGS PROFILE '${PROFILE_LOGIN}'"
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PROFILE_MT} SETTINGS merge_tree_min_bytes_for_wide_part MAX 100000"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER_MT} SETTINGS PROFILE '${PROFILE_MT}'"

echo 'the probe values differ from their declared defaults under compatibility 26.7'
# If either 26.8 history row is ever dropped, this fails loudly instead of leaving the arms below vacuous.
U=$(session_url a0)
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7'"
${CLICKHOUSE_CURL} -sS "$U" -d "SELECT name, value != default FROM system.settings WHERE name IN ('${P}', '${Q}', '${R}') ORDER BY name"

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

echo 'a merge_tree_-prefixed name that Settings owns is checked against its derived value too'
# The prefix alone does not say which class owns the name, so a prefix test would read the declared
# 65536 here instead of the derived 0 and let the reset escape the constraint. The assignment keeps
# the setting changed, so `compatibility` leaves it alone and the reset is the only route to 0.
U=$(user_session_url a7 "${USER_STREAM}")
${CLICKHOUSE_CURL} -sS "$U" -d "SET ${R} = 65536"
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7'"
${CLICKHOUSE_CURL} -sS "$U" -d "SET ${R} = DEFAULT" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
read_setting "$U" "${R}"

echo 'the reset rides along in the statement that activates compatibility'
# The value the reset lands on follows the `compatibility` the same statement carries, which the check
# before the statement cannot see. The refusal comes from the derived value instead, and the empty
# `compatibility` read afterwards is what shows the statement was undone rather than half applied.
U=$(user_session_url a8 "${USER_MIN}")
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7', ${Q} = DEFAULT" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
read_setting "$U" "${Q}"
read_setting "$U" compatibility

echo 'and the same statement is accepted when its compatibility moves the reset onto an allowed value'
# The other direction of the arm above: the reset lands on 1 under the 26.8 the statement carries, which is
# what a direct assignment of 1 is allowed to do, so the statement has to go through.
U=$(user_session_url a8f "${USER_MIN}")
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7'"
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.8', ${Q} = DEFAULT" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
read_setting "$U" "${Q}"
read_setting "$U" compatibility

echo 'a query-level compatibility override does not decide what the session reset lands on'
# The reset target is the session, at 26.7; under the query`s own 26.8 the setting would land on the
# allowed 1. Reading the session value afterwards is what proves which of the two was used.
U=$(user_session_url a8b "${USER_MIN}")
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7'"
${CLICKHOUSE_CURL} -sS "${U}&compatibility=26.8" -d "SET ${Q} = DEFAULT" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
read_setting "$U" "${Q}"

echo 'a reset that lands on the same value still leaves the setting under the constraint'
# That reset is allowed: it lands on 1. It also clears `changed`, so `compatibility` may now derive the
# setting, and the derived 0 has to be refused just as an assignment of 0 is - with the setting and the
# `compatibility` that would have derived it both left as they were.
U=$(user_session_url a8c "${USER_MIN}")
${CLICKHOUSE_CURL} -sS "$U" -d "SET ${Q} = DEFAULT"
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7'" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
read_setting "$U" "${Q}"
read_setting "$U" compatibility

echo 'control: without that reset the profile value keeps compatibility away from the setting'
U=$(user_session_url a8d "${USER_MIN}")
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7'"
read_setting "$U" "${Q}"
read_setting "$U" compatibility

echo 'a compatibility change is refused when it derives a value the profile forbids, with no reset at all'
# The profile constrains the setting without assigning it, so `compatibility` is free to derive it.
U=$(user_session_url a8e "${USER_STREAM}")
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7'" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
read_setting "$U" "${R}"
read_setting "$U" compatibility

echo 'the same refusal when compatibility arrives as a query setting rather than a statement'
# Two more carriers of a `compatibility` that derives a forbidden value, neither of them a `SET`: the HTTP
# URL, which applies it to the query context, and the native protocol, where the client sends it along with
# the query. Both reject the query rather than running it with the value the profile forbids.
${CLICKHOUSE_CURL} -sS "$(user_session_url a13 "${USER_STREAM}")&compatibility=26.7" -d "SELECT 1" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
${CLICKHOUSE_CLIENT} --user "${USER_STREAM}" --compatibility 26.7 -q "SELECT 1" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1

echo 'a value the profile itself derived at login does not fail the statements that follow'
# The profile carries both a `compatibility` and a constraint on a setting that version moves, so login
# derives a value the constraint forbids - and a profile is applied without checking the constraints. Only
# what a statement moves is that statement`s to answer for, or every later statement would fail.
U=$(user_session_url a12 "${USER_LOGIN}")
read_setting "$U" "${Q}"
${CLICKHOUSE_CURL} -sS "$U" -d "SET max_threads = DEFAULT" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7'" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
${CLICKHOUSE_CURL} -sS "$U" -d "SELECT 1"

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

echo 'control: each half of that statement is accepted on its own'
# The assignment is what makes the reset below move the setting: without it the reset lands on the value
# the setting already holds, and a change that changes nothing is permitted in readonly mode too.
U=$(session_url a14)
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7'"
${CLICKHOUSE_CURL} -sS "$U" -d "SET ${P} = 0"
${CLICKHOUSE_CURL} -sS "$U" -d "SET ${P} = DEFAULT"
read_setting "$U" "${P}"
${CLICKHOUSE_CURL} -sS "$U" -d "SET readonly = 1"
read_setting "$U" readonly

echo 'a reset is refused when the same statement enters readonly mode'
# The reset is checked against the state the statement`s own changes leave behind, so the readonly mode
# they enter applies to it. The reads afterwards show the refused statement left neither half in place.
U=$(session_url a15)
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7'"
${CLICKHOUSE_CURL} -sS "$U" -d "SET ${P} = 0"
${CLICKHOUSE_CURL} -sS "$U" -d "SET readonly = 1, ${P} = DEFAULT" 2>&1 | grep -o 'READONLY' | head -1
read_setting "$U" readonly
read_setting "$U" "${P}"

echo 'and a declared constraint does not change that answer'
# The two routes used to disagree here: the reset was checked before the statement`s changes when nothing
# was constrained and after them when something was, so the same statement was accepted for one user and
# refused for the other.
U=$(user_session_url a15b "${USER_MIN}")
${CLICKHOUSE_CURL} -sS "$U" -d "SET ${P} = 1"
${CLICKHOUSE_CURL} -sS "$U" -d "SET readonly = 1, ${P} = DEFAULT" 2>&1 | grep -o 'READONLY' | head -1
read_setting "$U" readonly
read_setting "$U" "${P}"

echo 'a compatibility carried by a CREATE settings clause is checked the same way'
# The clause is not an engine setting, so it is moved to the context from there rather than reaching it
# through a `SET`. The control carries a version that derives an allowed value, so the refusal cannot be
# the grant, the engine or the clause itself.
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}.* TO ${USER_STREAM}"
U=$(user_session_url a16 "${USER_STREAM}")
${CLICKHOUSE_CURL} -sS "$U" -d "CREATE TABLE ${CLICKHOUSE_DATABASE}.t1_05047 (x Int) ENGINE = MergeTree ORDER BY x SETTINGS compatibility = '26.7'" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
${CLICKHOUSE_CURL} -sS "$U" -d "CREATE TABLE ${CLICKHOUSE_DATABASE}.t2_05047 (x Int) ENGINE = MergeTree ORDER BY x SETTINGS compatibility = '26.8'"
${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE 't%\\_05047' ORDER BY name"

echo 'a compatibility carried by BACKUP core settings is checked the same way'
# `BACKUP`/`RESTORE` keep their settings outside `settings_ast`, so they reach the context by a route of
# their own. The check runs before the backup starts, which the control shows: without the setting the
# same statement gets as far as the privilege check instead.
U=$(user_session_url a17 "${USER_STREAM}")
${CLICKHOUSE_CURL} -sS "$U" -d "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t2_05047 TO Disk('backups', 'b_05047') SETTINGS compatibility = '26.7'" 2>&1 | grep -oE 'SETTING_CONSTRAINT_VIOLATION|ACCESS_DENIED' | head -1
${CLICKHOUSE_CURL} -sS "$U" -d "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t2_05047 TO Disk('backups', 'b_05047')" 2>&1 | grep -oE 'SETTING_CONSTRAINT_VIOLATION|ACCESS_DENIED' | head -1

echo 'a profile constraint on a MergeTree setting leaves an unrelated reset alone'
# What a profile constrains is not always a `Settings` name. Reading a `MergeTreeSettings` one off the
# session settings throws rather than reporting it absent, so the check on values nothing assigned has to
# skip it - and a reset under such a profile has to go through and land on the era value like any other.
U=$(user_session_url a18 "${USER_MT}")
${CLICKHOUSE_CURL} -sS "$U" -d "SET compatibility = '26.7'"
${CLICKHOUSE_CURL} -sS "$U" -d "SET ${P} = 0"
${CLICKHOUSE_CURL} -sS "$U" -d "SET ${P} = DEFAULT"
read_setting "$U" "${P}"

${CLICKHOUSE_CLIENT} -q "DROP USER ${USER_MIN}, ${USER_CONST}, ${USER_STREAM}, ${USER_LOGIN}, ${USER_MT}"
${CLICKHOUSE_CLIENT} -q "DROP PROFILE ${PROFILE_MIN}, ${PROFILE_CONST}, ${PROFILE_STREAM}, ${PROFILE_LOGIN}, ${PROFILE_MT}"
