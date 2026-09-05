#!/usr/bin/env bash

# `SET <name> = DEFAULT` used to bypass the settings constraints completely: a reset is not a setting
# change, so it never reached `SettingsConstraints`. With `max_query_size = 1000 MAX 1000` in the
# profile, `SET max_query_size = 2000` was rejected while `SET max_query_size = DEFAULT` silently
# restored the much larger built-in default. A `CONST` constraint was escapable the same way, and so
# was readonly mode: `SET readonly = 0` was rejected but `SET readonly = DEFAULT` left readonly mode.
#
# A reset that does not change the value stays allowed, so that resetting an untouched setting keeps
# working under a `CONST` constraint or in readonly mode.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_MAX="u_max_05023_${CLICKHOUSE_DATABASE}"
USER_CONST="u_const_05023_${CLICKHOUSE_DATABASE}"
USER_NOOP="u_noop_05023_${CLICKHOUSE_DATABASE}"
PROFILE_MAX="p_max_05023_${CLICKHOUSE_DATABASE}"
PROFILE_CONST="p_const_05023_${CLICKHOUSE_DATABASE}"
PROFILE_NOOP="p_noop_05023_${CLICKHOUSE_DATABASE}"

# `max_query_size` is used throughout because its declared default (262144) is far above the maximum
# the profiles below allow, and because the test runner never randomizes it.
DEFAULT_MAX_QUERY_SIZE=$(${CLICKHOUSE_CLIENT} -q "SELECT default FROM system.settings WHERE name = 'max_query_size'")

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER_MAX}, ${USER_CONST}, ${USER_NOOP}"
${CLICKHOUSE_CLIENT} -q "DROP PROFILE IF EXISTS ${PROFILE_MAX}, ${PROFILE_CONST}, ${PROFILE_NOOP}"
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PROFILE_MAX} SETTINGS max_query_size = 1000 MAX 1000"
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PROFILE_CONST} SETTINGS max_query_size = 1000 CONST"
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PROFILE_NOOP} SETTINGS max_query_size = ${DEFAULT_MAX_QUERY_SIZE} CONST"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER_MAX} SETTINGS PROFILE '${PROFILE_MAX}'"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER_CONST} SETTINGS PROFILE '${PROFILE_CONST}'"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER_NOOP} SETTINGS PROFILE '${PROFILE_NOOP}'"

echo 'MAX constraint'
# Assigning a value above the maximum was always rejected
${CLICKHOUSE_CLIENT} --user="${USER_MAX}" -q "SET max_query_size = 2000" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
# Resetting to the (larger) default has to be rejected as well, in both spellings
${CLICKHOUSE_CLIENT} --user="${USER_MAX}" -q "SET max_query_size = DEFAULT" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
${CLICKHOUSE_CLIENT} --user="${USER_MAX}" -q "SELECT 1 SETTINGS max_query_size = DEFAULT" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
# and it must leave the value alone
${CLICKHOUSE_CLIENT} --user="${USER_MAX}" -q "SELECT getSetting('max_query_size')"

echo 'CONST constraint'
${CLICKHOUSE_CLIENT} --user="${USER_CONST}" -q "SET max_query_size = DEFAULT" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1
${CLICKHOUSE_CLIENT} --user="${USER_CONST}" -q "SELECT 1 SETTINGS max_query_size = DEFAULT" 2>&1 | grep -o 'SETTING_CONSTRAINT_VIOLATION' | head -1

echo 'readonly mode'
# `readonly = 2` keeps every other setting changeable, so the settings the test runner randomizes do
# not run into the readonly check themselves and the error below can only come from `readonly`.
# Assigning to `readonly` was always rejected...
${CLICKHOUSE_CLIENT} -q "SET readonly = 2; SET readonly = 0" 2>&1 | grep -o "Cannot modify 'readonly' setting in readonly mode" | head -1
# ...and so must resetting it, which used to be a way out of readonly mode
${CLICKHOUSE_CLIENT} -q "SET readonly = 2; SET readonly = DEFAULT" 2>&1 | grep -o "Cannot modify 'readonly' setting in readonly mode" | head -1
${CLICKHOUSE_CLIENT} -q "SET readonly = 2; SELECT 1 SETTINGS readonly = DEFAULT" 2>&1 | grep -o "Cannot modify 'readonly' setting in readonly mode" | head -1

echo 'a reset that changes nothing is still allowed'
# The profile pins `max_query_size` to its declared default and makes it CONST, so the reset is a
# no-op and must not be reported
${CLICKHOUSE_CLIENT} --user="${USER_NOOP}" -q "SET max_query_size = DEFAULT; SELECT 'allowed'"

echo 'settings without a declared default are unaffected'
# A custom setting is dropped rather than reset, and cannot carry a value constraint.
${CLICKHOUSE_CLIENT} -q "SET SQL_probe_05023 = 1; SET SQL_probe_05023 = DEFAULT; SELECT 'custom setting reset'"
# It is still a session-state mutation, so readonly mode must reject the reset. Unlike `readonly = 2`
# above, `readonly = 1` rejects every setting change, including the ones the client and the test
# runner send along with each statement (`send_logs_level`, `log_comment`, the randomized settings),
# which would be rejected before the reset under test. The check therefore goes over HTTP with a
# session and a bare URL that carries nothing but the session id.
SESSION="s_05023_${CLICKHOUSE_DATABASE}_$$"
SESSION_URL="${CLICKHOUSE_URL%%\?*}?session_id=${SESSION}"
${CLICKHOUSE_CURL} -sS "${SESSION_URL}" -d "SET SQL_probe_05023 = 1"
${CLICKHOUSE_CURL} -sS "${SESSION_URL}" -d "SET readonly = 1"
${CLICKHOUSE_CURL} -sS "${SESSION_URL}" -d "SET SQL_probe_05023 = DEFAULT" 2>&1 | grep -o "Cannot modify 'SQL_probe_05023' setting in readonly mode" | head -1
# An unknown setting is still silently ignored rather than reported
${CLICKHOUSE_CLIENT} -q "SET nonexistent_setting_05023 = DEFAULT; SELECT 'unknown setting ignored'"

${CLICKHOUSE_CLIENT} -q "DROP USER ${USER_MAX}, ${USER_CONST}, ${USER_NOOP}"
${CLICKHOUSE_CLIENT} -q "DROP PROFILE ${PROFILE_MAX}, ${PROFILE_CONST}, ${PROFILE_NOOP}"
