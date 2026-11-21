#!/usr/bin/env bash
# HTTP URL parameters are a settings change list in URL order, so a `profile` parameter constrains the
# parameters after it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PROFILE="profile_http_$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT -m -q "
DROP SETTINGS PROFILE IF EXISTS $PROFILE;
CREATE SETTINGS PROFILE $PROFILE SETTINGS max_execution_time = 10 CONST, max_result_rows = 12345;
"

function run_with_params()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&$1" -d "$2"
}

echo '-- a parameter after the profile is checked against the constraints it installs'
run_with_params "profile=$PROFILE&max_execution_time=999" "SELECT 1" | grep -o -m1 SETTING_CONSTRAINT_VIOLATION
echo '-- a parameter before the profile is overridden by it'
run_with_params "max_result_rows=8&profile=$PROFILE" "SELECT getSetting('max_result_rows')"
echo '-- a parameter equal to the value before the profile still overrides the profile'
run_with_params "profile=$PROFILE&max_result_rows=0" "SELECT getSetting('max_result_rows')"

$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE $PROFILE"
