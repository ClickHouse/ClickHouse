#!/usr/bin/env bash
# Tags: no-random-settings
# no-random-settings: the framework injects settings into CLICKHOUSE_URL, which would collide with the settings under test

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

profile="profile_${CLICKHOUSE_DATABASE}"
profile_bg="profile_bg_${CLICKHOUSE_DATABASE}"
profile_ms="profile_ms_${CLICKHOUSE_DATABASE}"
profile_compat="profile_compat_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
    CREATE SETTINGS PROFILE ${profile} SETTINGS wait_for_async_insert = 0, max_result_rows = 5;
    CREATE SETTINGS PROFILE ${profile_bg} SETTINGS run_query_in_background = 1;
    CREATE SETTINGS PROFILE ${profile_ms} SETTINGS max_sessions_for_user = 1;
    CREATE SETTINGS PROFILE ${profile_compat} SETTINGS compatibility = '24.12';
"

Q="SELECT getSetting('wait_for_async_insert'), getSetting('max_result_rows')"

echo "-- no profile"
# max_result_rows is given a non-default value by the CI test configuration, so assert only what
# the arms below need of the pre-profile value: it is neither the profile's 5 nor the explicit 7.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" \
    -d "SELECT getSetting('wait_for_async_insert'), toBool(getSetting('max_result_rows') NOT IN (5, 7))"

echo "-- the profile alone moves both settings"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&profile=${profile}" -d "$Q"

echo "-- an explicit value equal to the pre-profile value survives"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&profile=${profile}&wait_for_async_insert=1" -d "$Q"

echo "-- an explicit value different from the pre-profile value survives"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&profile=${profile}&max_result_rows=7" -d "$Q"

echo "-- both at once"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&profile=${profile}&wait_for_async_insert=1&max_result_rows=7" -d "$Q"

echo "-- a profile after the setting still wins"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&wait_for_async_insert=1&profile=${profile}" -d "$Q"

echo "-- a profile carrying compatibility still reverts the setting on its own"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&profile=${profile_compat}" \
    -d "SELECT getSetting('use_skip_indexes_if_final')"

echo "-- but it does not revert a value the same request set explicitly"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&use_skip_indexes_if_final=1&profile=${profile_compat}" \
    -d "SELECT getSetting('use_skip_indexes_if_final')"

echo "-- an explicit run_query_in_background=0 keeps the query in the foreground"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&profile=${profile_bg}&run_query_in_background=0" -d "SELECT 42"

echo "-- SET in one statement is unaffected"
${CLICKHOUSE_CLIENT} -m -q "SET profile = '${profile}', wait_for_async_insert = 1; $Q"

echo "-- a profile in a subquery's SETTINGS clause keeps the explicit values too"
# `max_result_rows` is left to the profile, so the row changes too if the clause is never applied.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" \
    -d "SELECT * FROM ($Q SETTINGS profile = '${profile}', wait_for_async_insert = 1)"

echo "-- a setting a query may not set is not restored over the profile"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&profile=${profile_ms}&max_sessions_for_user=0" \
    -d "SELECT getSetting('max_sessions_for_user')"

echo "-- and a genuine attempt to set it from a query is still rejected"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&profile=${profile_ms}&max_sessions_for_user=5" \
    -d "SELECT 1" 2>&1 | grep -o -m1 "READONLY"

${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE ${profile}, ${profile_bg}, ${profile_ms}, ${profile_compat}"
