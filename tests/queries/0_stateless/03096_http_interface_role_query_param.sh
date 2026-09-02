#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TEST_USER="03096_role_query_param_user"
TEST_USER_AUTH="$TEST_USER:"

TEST_ROLE1="03096_role_query_param_role1"
TEST_ROLE2="03096_role_query_param_role2"
TEST_ROLE_ENABLED_BY_DEFAULT="03096_role_query_param_role_enabled_by_default"
TEST_ROLE_NOT_GRANTED="03096_role_query_param_role_not_granted"
TEST_ROLE_SPECIAL_CHARS="\`03096_role_query_param_@!\\$\`" # = CREATE ROLE `03096_role_query_param_@!\$`
TEST_ROLE_SPECIAL_CHARS_URLENCODED="03096_role_query_param_%40!%5C%24"

CHANGED_SETTING_NAME="max_result_rows"
CHANGED_SETTING_VALUE="42"

SHOW_CURRENT_ROLES_QUERY="SELECT role_name FROM system.current_roles ORDER BY role_name ASC"
SHOW_CHANGED_SETTINGS_QUERY="SELECT name, value FROM system.settings WHERE changed = 1 AND name = '$CHANGED_SETTING_NAME' ORDER BY name ASC"

$CLICKHOUSE_CLIENT --query "
DROP USER IF EXISTS $TEST_USER;
DROP ROLE IF EXISTS $TEST_ROLE1;
DROP ROLE IF EXISTS $TEST_ROLE2;
DROP ROLE IF EXISTS $TEST_ROLE_ENABLED_BY_DEFAULT;
DROP ROLE IF EXISTS $TEST_ROLE_NOT_GRANTED;
DROP ROLE IF EXISTS $TEST_ROLE_SPECIAL_CHARS;
CREATE USER $TEST_USER NOT IDENTIFIED;
CREATE ROLE $TEST_ROLE_ENABLED_BY_DEFAULT;
GRANT $TEST_ROLE_ENABLED_BY_DEFAULT TO $TEST_USER;
SET DEFAULT ROLE $TEST_ROLE_ENABLED_BY_DEFAULT TO $TEST_USER;
CREATE ROLE $TEST_ROLE1;
GRANT $TEST_ROLE1 TO $TEST_USER;
CREATE ROLE $TEST_ROLE2;
GRANT $TEST_ROLE2 TO $TEST_USER;
CREATE ROLE $TEST_ROLE_SPECIAL_CHARS;
GRANT $TEST_ROLE_SPECIAL_CHARS TO $TEST_USER;
CREATE ROLE $TEST_ROLE_NOT_GRANTED;
"

echo "### Shows the default role when there are no role parameters"
$CLICKHOUSE_CURL -u $TEST_USER_AUTH -sS "$CLICKHOUSE_URL" --data-binary "$SHOW_CURRENT_ROLES_QUERY"

echo "### Shows a single role from the query parameters"
$CLICKHOUSE_CURL -u $TEST_USER_AUTH -sS "$CLICKHOUSE_URL&role=$TEST_ROLE1" --data-binary "$SHOW_CURRENT_ROLES_QUERY"

echo "### Shows multiple roles from the query parameters"
$CLICKHOUSE_CURL -u $TEST_USER_AUTH -sS "$CLICKHOUSE_URL&role=$TEST_ROLE1&role=$TEST_ROLE2" --data-binary "$SHOW_CURRENT_ROLES_QUERY"

echo "### Sets the default role alongside with another granted one"
$CLICKHOUSE_CURL -u $TEST_USER_AUTH -sS "$CLICKHOUSE_URL&role=$TEST_ROLE_ENABLED_BY_DEFAULT&role=$TEST_ROLE1" --data-binary "$SHOW_CURRENT_ROLES_QUERY"

echo "### Sets a role with special characters in the name"
$CLICKHOUSE_CURL -u $TEST_USER_AUTH -sS "$CLICKHOUSE_URL&role=$TEST_ROLE_SPECIAL_CHARS_URLENCODED" --data-binary "$SHOW_CURRENT_ROLES_QUERY"

echo "### Sets a role with special characters in the name with another granted role"
$CLICKHOUSE_CURL -u $TEST_USER_AUTH -sS "$CLICKHOUSE_URL&role=$TEST_ROLE_SPECIAL_CHARS_URLENCODED&role=$TEST_ROLE1" --data-binary "$SHOW_CURRENT_ROLES_QUERY"

echo "### Sets a role once when it's present in the query parameters multiple times"
$CLICKHOUSE_CURL -u $TEST_USER_AUTH -sS "$CLICKHOUSE_URL&role=$TEST_ROLE1&role=$TEST_ROLE1" --data-binary "$SHOW_CURRENT_ROLES_QUERY"

echo "### Sets a role when there are other parameters in the query (before the role parameter)"
$CLICKHOUSE_CURL -u $TEST_USER_AUTH -sS "$CLICKHOUSE_URL&$CHANGED_SETTING_NAME=$CHANGED_SETTING_VALUE&role=$TEST_ROLE1" --data-binary "$SHOW_CURRENT_ROLES_QUERY"
$CLICKHOUSE_CURL -u $TEST_USER_AUTH -sS "$CLICKHOUSE_URL&$CHANGED_SETTING_NAME=$CHANGED_SETTING_VALUE&role=$TEST_ROLE1" --data-binary "$SHOW_CHANGED_SETTINGS_QUERY"

echo "### Sets a role when there are other parameters in the query (after the role parameter)"
$CLICKHOUSE_CURL -u $TEST_USER_AUTH -sS "$CLICKHOUSE_URL&role=$TEST_ROLE1&$CHANGED_SETTING_NAME=$CHANGED_SETTING_VALUE" --data-binary "$SHOW_CURRENT_ROLES_QUERY"
$CLICKHOUSE_CURL -u $TEST_USER_AUTH -sS "$CLICKHOUSE_URL&role=$TEST_ROLE1&$CHANGED_SETTING_NAME=$CHANGED_SETTING_VALUE" --data-binary "$SHOW_CHANGED_SETTINGS_QUERY"

echo "### Sets multiple roles when there are other parameters in the query"
$CLICKHOUSE_CURL -u $TEST_USER_AUTH -sS "$CLICKHOUSE_URL&role=$TEST_ROLE1&$CHANGED_SETTING_NAME=$CHANGED_SETTING_VALUE&role=$TEST_ROLE2" --data-binary "$SHOW_CURRENT_ROLES_QUERY"
$CLICKHOUSE_CURL -u $TEST_USER_AUTH -sS "$CLICKHOUSE_URL&role=$TEST_ROLE1&$CHANGED_SETTING_NAME=$CHANGED_SETTING_VALUE&role=$TEST_ROLE2" --data-binary "$SHOW_CHANGED_SETTINGS_QUERY"

echo "### Cannot set a role that does not exist (single parameter)"
OUT=$($CLICKHOUSE_CURL -u $TEST_USER_AUTH -sS "$CLICKHOUSE_URL&role=aaaaaaaaaaa" --data-binary "$SHOW_CURRENT_ROLES_QUERY")
echo -ne $OUT | grep -o "Code: 511"     || echo "expected code 511, got: $OUT"
echo -ne $OUT | grep -o "UNKNOWN_ROLE"  || echo "expected UNKNOWN_ROLE error, got: $OUT"

echo "### Cannot set a role that does not exist (multiple parameters)"
OUT=$($CLICKHOUSE_CURL -u $TEST_USER_AUTH -sS "$CLICKHOUSE_URL&role=$TEST_ROLE1&role=aaaaaaaaaaa" --data-binary "$SHOW_CURRENT_ROLES_QUERY")
echo -ne $OUT | grep -o "Code: 511"     || echo "expected code 511, got: $OUT"
echo -ne $OUT | grep -o "UNKNOWN_ROLE"  || echo "expected UNKNOWN_ROLE error, got: $OUT"

echo "### Cannot set a role that is not granted to the user (single parameter)"
OUT=$($CLICKHOUSE_CURL -u $TEST_USER_AUTH -sS "$CLICKHOUSE_URL&role=$TEST_ROLE_NOT_GRANTED" --data-binary "$SHOW_CURRENT_ROLES_QUERY")
echo -ne $OUT | grep -o "Code: 512"            || echo "expected code 512, got: $OUT"
echo -ne $OUT | grep -o "SET_NON_GRANTED_ROLE" || echo "expected SET_NON_GRANTED_ROLE error, got: $OUT"

echo "### Cannot set a role that is not granted to the user (multiple parameters)"
OUT=$($CLICKHOUSE_CURL -u $TEST_USER_AUTH -sS "$CLICKHOUSE_URL&role=$TEST_ROLE1&role=$TEST_ROLE_NOT_GRANTED" --data-binary "$SHOW_CURRENT_ROLES_QUERY")
echo -ne $OUT | grep -o "Code: 512"            || echo "expected code 512, got: $OUT"
echo -ne $OUT | grep -o "SET_NON_GRANTED_ROLE" || echo "expected SET_NON_GRANTED_ROLE error, got: $OUT"

$CLICKHOUSE_CLIENT --query "
DROP USER $TEST_USER;
DROP ROLE $TEST_ROLE1;
DROP ROLE $TEST_ROLE2;
DROP ROLE $TEST_ROLE_ENABLED_BY_DEFAULT;
DROP ROLE $TEST_ROLE_NOT_GRANTED;
DROP ROLE $TEST_ROLE_SPECIAL_CHARS;
"
