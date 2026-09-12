#!/usr/bin/env bash

# A settings post-processor writes settings the request never names: enabling `make_distributed_plan`
# turns off the features a distributed query plan does not support. Nothing assigns those values, so
# the constraints in force are only ever answered for after the changes are applied.
#
# Every value is read back over HTTP with a persistent session and a bare URL, which keeps the test
# runner's randomized `compile_expressions` out of the session.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `compile_expressions` is one of the settings the plan turns off and its declared default is 1, the
# polarity a `MIN 1` constraint needs. `enable_parallel_replicas` is another one, reached through an
# alias: the constraint is declared on the alias and the post-processor writes the canonical name.
S=compile_expressions
A=enable_parallel_replicas

USER_PLAN="u_plan_05175_${CLICKHOUSE_DATABASE}"
PROFILE_MIN="p_min_05175_${CLICKHOUSE_DATABASE}"
PROFILE_MAX="p_max_05175_${CLICKHOUSE_DATABASE}"
PROFILE_ALIAS="p_alias_05175_${CLICKHOUSE_DATABASE}"
PROFILE_PLAN="p_plan_05175_${CLICKHOUSE_DATABASE}"

BASE_URL="${CLICKHOUSE_URL%%\?*}"
session_url() { echo "${BASE_URL}?session_id=s_05175_${CLICKHOUSE_DATABASE}_$$_$1"; }
user_session_url() { echo "${BASE_URL}?session_id=s_05175_${CLICKHOUSE_DATABASE}_$$_$1&user=$2"; }
read_setting() { ${CLICKHOUSE_CURL} -sS "$1" -d "SELECT value FROM system.settings WHERE name = '$2'"; }

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER_PLAN}"
${CLICKHOUSE_CLIENT} -q "DROP PROFILE IF EXISTS ${PROFILE_MIN}, ${PROFILE_MAX}, ${PROFILE_ALIAS}, ${PROFILE_PLAN}"
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PROFILE_MIN} SETTINGS ${S} MIN 1"
# The opposite polarity: it allows the value the plan writes and forbids the declared default.
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PROFILE_MAX} SETTINGS ${S} MAX 0"
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PROFILE_ALIAS} SETTINGS ${A} MIN 1"
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PROFILE_PLAN} SETTINGS make_distributed_plan = 1, ${S} MIN 1"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER_PLAN} SETTINGS PROFILE '${PROFILE_PLAN}'"

echo 'the plan moves the probe off the value the constraints below require'
# If it ever stops doing that, every refusal below disappears and the arms turn vacuous.
U=$(session_url a0)
${CLICKHOUSE_CURL} -sS "$U" -d "SET make_distributed_plan = 1"
read_setting "$U" "${S}"
read_setting "$U" make_distributed_plan

echo 'assigning the probe that value is refused'
# The reference behaviour: what the plan writes is a value this session may not write itself.
U=$(session_url a1)
${CLICKHOUSE_CURL} -sS "$U" -d "SET profile = '${PROFILE_MIN}'"
${CLICKHOUSE_CURL} -sS "$U" -d "SET ${S} = 0" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
read_setting "$U" "${S}"

echo 'so enabling the plan is refused as well'
# The statement names `make_distributed_plan`, which nothing constrains, and the check before it can only
# see that. The refusal has to come from the value the post-processor leaves behind. The reads show the
# statement was undone, and the profile read shows the undo put the profile pointer back too.
U=$(session_url a2)
${CLICKHOUSE_CURL} -sS "$U" -d "SET profile = '${PROFILE_MIN}'"
${CLICKHOUSE_CURL} -sS "$U" -d "SET make_distributed_plan = 1" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
read_setting "$U" "${S}"
read_setting "$U" make_distributed_plan
${CLICKHOUSE_CURL} -sS "$U" -d "SELECT has(currentProfiles(), '${PROFILE_MIN}')"

echo 'and the same setting carried by the URL is refused too'
U=$(session_url a3)
${CLICKHOUSE_CURL} -sS "$U" -d "SET profile = '${PROFILE_MIN}'"
${CLICKHOUSE_CURL} -sS "$(session_url a3)&make_distributed_plan=1" -d "SELECT 1" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
read_setting "$U" "${S}"

echo 'and in a query-level SETTINGS clause'
U=$(session_url a4)
${CLICKHOUSE_CURL} -sS "$U" -d "SET profile = '${PROFILE_MIN}'"
${CLICKHOUSE_CURL} -sS "$U" -d "SELECT 1 SETTINGS make_distributed_plan = 1" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
read_setting "$U" "${S}"

echo 'a profile switch is refused when its constraint forbids a value the plan already wrote'
# Here the switch moves nothing: the probe already holds the value the plan wrote and what arrives is the
# constraint alone. The value under it is still one nothing asked for, so the switch answers for it. The
# reads show the refusal left the profile out.
U=$(session_url a5)
${CLICKHOUSE_CURL} -sS "$U" -d "SET make_distributed_plan = 1"
${CLICKHOUSE_CURL} -sS "$U" -d "SET profile = '${PROFILE_MIN}'" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
read_setting "$U" "${S}"
${CLICKHOUSE_CURL} -sS "$U" -d "SELECT has(currentProfiles(), '${PROFILE_MIN}')"

echo 'a value assigned earlier is not exempt, and an alias-declared constraint applies'
# The assignment is accepted, so the session holds a value it is allowed to hold. Enabling the plan then
# takes it away, which is what makes this a value the request has to answer for rather than one it asked
# for. The constraint is declared on the alias while the post-processor writes the canonical name.
U=$(session_url a6)
${CLICKHOUSE_CURL} -sS "$U" -d "SET profile = '${PROFILE_ALIAS}'"
${CLICKHOUSE_CURL} -sS "$U" -d "SET ${A} = 1"
read_setting "$U" "${A}"
${CLICKHOUSE_CURL} -sS "$U" -d "SET make_distributed_plan = 1" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
read_setting "$U" "${A}"
read_setting "$U" make_distributed_plan

echo 'control: a constraint that allows the value the plan writes accepts it'
# Same setting, same post-processor, only what is allowed differs. So the refusals above are the
# constraint on the value and not the plan being enabled.
U=$(session_url a7)
${CLICKHOUSE_CURL} -sS "$U" -d "SET profile = '${PROFILE_MAX}'"
${CLICKHOUSE_CURL} -sS "$U" -d "SET make_distributed_plan = 1" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
read_setting "$U" "${S}"
read_setting "$U" make_distributed_plan

echo 'control: a login profile that enables the plan and constrains what it moves stays usable'
# The login applies the profile without checking it, so the session starts on the forbidden value. Later
# statements move nothing, and refusing them would leave the user unable to run anything at all - an
# assignment and a reset of an unrelated setting are both accepted.
U=$(user_session_url a8 "${USER_PLAN}")
read_setting "$U" "${S}"
${CLICKHOUSE_CURL} -sS "$U" -d "SET max_block_size = 1000"
${CLICKHOUSE_CURL} -sS "$U" -d "SET max_block_size = DEFAULT"
read_setting "$U" "${S}"
${CLICKHOUSE_CURL} -sS "$U" -d "SELECT has(currentProfiles(), '${PROFILE_PLAN}')"

${CLICKHOUSE_CLIENT} -q "DROP USER ${USER_PLAN}"
${CLICKHOUSE_CLIENT} -q "DROP PROFILE ${PROFILE_MIN}, ${PROFILE_MAX}, ${PROFILE_ALIAS}, ${PROFILE_PLAN}"
