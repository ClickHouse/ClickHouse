#!/usr/bin/env bash
# HTTP URL parameters are a settings change list like any other, so a `profile` parameter must
# constrain the parameters that follow it.

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

$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE $PROFILE"
