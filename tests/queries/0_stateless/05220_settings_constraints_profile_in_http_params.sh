#!/usr/bin/env bash
# HTTP URL parameters are a settings change list like any other, so a `profile` parameter must
# constrain the other parameters, wherever they sit relative to it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PROFILE="profile_http_$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS $PROFILE"
$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE $PROFILE SETTINGS max_execution_time = 10 CONST"

echo '-- a value passed after the profile parameter is rejected'
${CLICKHOUSE_CURL} -sS -X POST --data-binary @- "${CLICKHOUSE_URL}&profile=$PROFILE&max_execution_time=999" <<< "SELECT 1" | grep -o -m1 SETTING_CONSTRAINT_VIOLATION

echo '-- the profile alone still applies'
${CLICKHOUSE_CURL} -sS -X POST --data-binary @- "${CLICKHOUSE_URL}&profile=$PROFILE" <<< "SELECT getSetting('max_execution_time')"

MEMORY_PROFILE="profile_http_memory_$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS $MEMORY_PROFILE"
$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE $MEMORY_PROFILE SETTINGS max_memory_usage MAX 10000000000"

echo '-- a value passed before the profile parameter is rejected too'
${CLICKHOUSE_CURL} -sS -X POST --data-binary @- "${CLICKHOUSE_URL}&max_memory_usage=1099511627776&profile=$MEMORY_PROFILE" <<< "SELECT 1" | grep -o -m1 SETTING_CONSTRAINT_VIOLATION

echo '-- a parameter repeated around the profile is rejected on its last value'
${CLICKHOUSE_CURL} -sS -X POST --data-binary @- "${CLICKHOUSE_URL}&max_memory_usage=7&profile=$MEMORY_PROFILE&max_memory_usage=1099511627776" <<< "SELECT 1" | grep -o -m1 SETTING_CONSTRAINT_VIOLATION

echo '-- a value within the constraint is still accepted'
${CLICKHOUSE_CURL} -sS -X POST --data-binary @- "${CLICKHOUSE_URL}&profile=$MEMORY_PROFILE&max_memory_usage=1000000000" <<< "SELECT getSetting('max_memory_usage')"

ROWS_PROFILE="profile_http_rows_$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS $ROWS_PROFILE"
$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE $ROWS_PROFILE SETTINGS max_result_rows = 12345"

echo '-- a parameter equal to the value before the profile still overrides the profile'
current=$(${CLICKHOUSE_CURL} -sS -X POST --data-binary @- "${CLICKHOUSE_URL}" <<< "SELECT getSetting('max_result_rows')")
result=$(${CLICKHOUSE_CURL} -sS -X POST --data-binary @- "${CLICKHOUSE_URL}&profile=$ROWS_PROFILE&max_result_rows=$current" <<< "SELECT getSetting('max_result_rows')")
[[ "$result" == "$current" ]] && echo "OK" || echo "expected $current, got $result"

$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE $PROFILE"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE $MEMORY_PROFILE"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE $ROWS_PROFILE"
