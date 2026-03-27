#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Use CLICKHOUSE_DATABASE as a unique suffix to avoid collisions when the test
# runs in parallel (handlers are server-global, not per-database).
SUFFIX="${CLICKHOUSE_DATABASE}"

SETTINGS="--allow_experimental_sql_handlers=1"

# Base URL for HTTP requests (without query params from CLICKHOUSE_URL)
HTTP_BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

## Part 1: DDL tests

# Basic CREATE / DROP
$CLICKHOUSE_CLIENT $SETTINGS -q "CREATE HANDLER test_h1_${SUFFIX} URL '/api/test1_${SUFFIX}' AS 'SELECT 1'"
$CLICKHOUSE_CLIENT $SETTINGS -q "DROP HANDLER test_h1_${SUFFIX}"

# CREATE HANDLER IF NOT EXISTS
$CLICKHOUSE_CLIENT $SETTINGS -q "CREATE HANDLER test_h2_${SUFFIX} URL '/api/test2_${SUFFIX}' AS 'SELECT 2'"
$CLICKHOUSE_CLIENT $SETTINGS -q "CREATE HANDLER IF NOT EXISTS test_h2_${SUFFIX} URL '/api/test2_${SUFFIX}' AS 'SELECT 2'"
$CLICKHOUSE_CLIENT $SETTINGS -q "DROP HANDLER test_h2_${SUFFIX}"

# DROP HANDLER IF EXISTS (should not throw)
$CLICKHOUSE_CLIENT $SETTINGS -q "DROP HANDLER IF EXISTS test_h_nonexistent_${SUFFIX}"

# ALTER HANDLER IF EXISTS on non-existent (should not throw)
$CLICKHOUSE_CLIENT $SETTINGS -q "ALTER HANDLER IF EXISTS test_h_nonexistent_${SUFFIX} URL '/test' AS 'SELECT 0'"

# DROP HANDLER error on non-existent
$CLICKHOUSE_CLIENT $SETTINGS -q "DROP HANDLER test_h_nonexistent_${SUFFIX}" 2>&1 | grep -m1 -o 'BAD_ARGUMENTS'

echo "DDL OK"

## Part 2: End-to-end HTTP handler tests (exact URL match)

$CLICKHOUSE_CLIENT $SETTINGS -q "CREATE HANDLER test_exact_${SUFFIX} URL '/e2e_exact_${SUFFIX}' METHODS (GET) AS 'SELECT 42 AS answer'"
RESULT=$($CLICKHOUSE_CURL "${HTTP_BASE}/e2e_exact_${SUFFIX}")
echo "exact: ${RESULT}"
$CLICKHOUSE_CLIENT $SETTINGS -q "DROP HANDLER test_exact_${SUFFIX}"

## Part 3: URL PREFIX match

$CLICKHOUSE_CLIENT $SETTINGS -q "CREATE HANDLER test_prefix_${SUFFIX} URL PREFIX '/e2e_pfx_${SUFFIX}' METHODS (GET) AS 'SELECT 100 AS pfx'"
RESULT=$($CLICKHOUSE_CURL "${HTTP_BASE}/e2e_pfx_${SUFFIX}/sub/path")
echo "prefix: ${RESULT}"
$CLICKHOUSE_CLIENT $SETTINGS -q "DROP HANDLER test_prefix_${SUFFIX}"

## Part 4: URL REGEXP match

$CLICKHOUSE_CLIENT $SETTINGS -q "CREATE HANDLER test_re_${SUFFIX} URL REGEXP '^/e2e_re_${SUFFIX}/v[0-9]+\$' METHODS (GET) AS 'SELECT 200 AS re'"
RESULT=$($CLICKHOUSE_CURL "${HTTP_BASE}/e2e_re_${SUFFIX}/v3")
echo "regexp: ${RESULT}"
$CLICKHOUSE_CLIENT $SETTINGS -q "DROP HANDLER test_re_${SUFFIX}"

## Part 5: ALTER HANDLER changes query

$CLICKHOUSE_CLIENT $SETTINGS -q "CREATE HANDLER test_alter_${SUFFIX} URL '/e2e_alter_${SUFFIX}' METHODS (GET) AS 'SELECT 1 AS before_alter'"
RESULT_BEFORE=$($CLICKHOUSE_CURL "${HTTP_BASE}/e2e_alter_${SUFFIX}")
$CLICKHOUSE_CLIENT $SETTINGS -q "ALTER HANDLER test_alter_${SUFFIX} AS 'SELECT 2 AS after_alter'"
RESULT_AFTER=$($CLICKHOUSE_CURL "${HTTP_BASE}/e2e_alter_${SUFFIX}")
echo "alter before: ${RESULT_BEFORE}"
echo "alter after: ${RESULT_AFTER}"
$CLICKHOUSE_CLIENT $SETTINGS -q "DROP HANDLER test_alter_${SUFFIX}"

## Part 6: Method filtering — POST should work

$CLICKHOUSE_CLIENT $SETTINGS -q "CREATE HANDLER test_method_${SUFFIX} URL '/e2e_method_${SUFFIX}' METHODS (POST) AS 'SELECT 300 AS post_only'"
RESULT_POST=$($CLICKHOUSE_CURL -X POST -d '' "${HTTP_BASE}/e2e_method_${SUFFIX}")
echo "method POST: ${RESULT_POST}"
# GET should NOT match a POST-only handler — response should not contain 300
RESULT_GET=$($CLICKHOUSE_CURL "${HTTP_BASE}/e2e_method_${SUFFIX}" 2>&1)
echo "method GET on POST-only: $(echo "${RESULT_GET}" | grep -c '^300$')"
$CLICKHOUSE_CLIENT $SETTINGS -q "DROP HANDLER test_method_${SUFFIX}"

## Part 7: Handler does not match wrong URL

$CLICKHOUSE_CLIENT $SETTINGS -q "CREATE HANDLER test_nomatch_${SUFFIX} URL '/e2e_nomatch_${SUFFIX}' METHODS (GET) AS 'SELECT 999'"
RESULT=$($CLICKHOUSE_CURL "${HTTP_BASE}/e2e_wrong_path_${SUFFIX}" 2>&1)
echo "wrong URL matches: $(echo "${RESULT}" | grep -c '^999$')"
$CLICKHOUSE_CLIENT $SETTINGS -q "DROP HANDLER test_nomatch_${SUFFIX}"

## Part 8: Experimental setting gate — DDL should fail without the setting

$CLICKHOUSE_CLIENT -q "CREATE HANDLER test_gate_${SUFFIX} URL '/gate' AS 'SELECT 1'" 2>&1 | grep -m1 -o 'SUPPORT_IS_DISABLED'

## Part 9: Handler removed after DROP — HTTP should no longer match

$CLICKHOUSE_CLIENT $SETTINGS -q "CREATE HANDLER test_drop_${SUFFIX} URL '/e2e_drop_${SUFFIX}' METHODS (GET) AS 'SELECT 777'"
RESULT_BEFORE=$($CLICKHOUSE_CURL "${HTTP_BASE}/e2e_drop_${SUFFIX}")
$CLICKHOUSE_CLIENT $SETTINGS -q "DROP HANDLER test_drop_${SUFFIX}"
RESULT_AFTER=$($CLICKHOUSE_CURL "${HTTP_BASE}/e2e_drop_${SUFFIX}" 2>&1)
echo "before drop: ${RESULT_BEFORE}"
echo "after drop matches: $(echo "${RESULT_AFTER}" | grep -c '^777$')"

## Part 10: Multiple METHODS (GET, POST)

$CLICKHOUSE_CLIENT $SETTINGS -q "CREATE HANDLER test_multi_${SUFFIX} URL '/e2e_multi_${SUFFIX}' METHODS (GET, POST) AS 'SELECT 500'"
RESULT_GET=$($CLICKHOUSE_CURL "${HTTP_BASE}/e2e_multi_${SUFFIX}")
RESULT_POST=$($CLICKHOUSE_CURL -X POST -d '' "${HTTP_BASE}/e2e_multi_${SUFFIX}")
echo "multi GET: ${RESULT_GET}"
echo "multi POST: ${RESULT_POST}"
$CLICKHOUSE_CLIENT $SETTINGS -q "DROP HANDLER test_multi_${SUFFIX}"

## Part 11: ALTER HANDLER with no clauses should fail

$CLICKHOUSE_CLIENT $SETTINGS -q "CREATE HANDLER test_noop_${SUFFIX} URL '/e2e_noop_${SUFFIX}' AS 'SELECT 1'"
$CLICKHOUSE_CLIENT $SETTINGS -q "ALTER HANDLER test_noop_${SUFFIX}" 2>&1 | grep -m1 -o 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT $SETTINGS -q "DROP HANDLER test_noop_${SUFFIX}"

## Part 12: Invalid method name should fail

$CLICKHOUSE_CLIENT $SETTINGS -q "CREATE HANDLER test_badmethod_${SUFFIX} URL '/e2e_badmethod_${SUFFIX}' METHODS (FOOBAR) AS 'SELECT 1'" 2>&1 | grep -m1 -o 'BAD_ARGUMENTS'

## Part 13: Invalid SQL in AS clause should fail

$CLICKHOUSE_CLIENT $SETTINGS -q "CREATE HANDLER test_badsql_${SUFFIX} URL '/e2e_badsql_${SUFFIX}' AS 'this is not SQL at all'" 2>&1 | grep -m1 -o 'SYNTAX_ERROR'

## Part 14: Duplicate exact URL should fail (ambiguity check)

$CLICKHOUSE_CLIENT $SETTINGS -q "CREATE HANDLER test_dup1_${SUFFIX} URL '/e2e_dup_${SUFFIX}' AS 'SELECT 1'"
$CLICKHOUSE_CLIENT $SETTINGS -q "CREATE HANDLER test_dup2_${SUFFIX} URL '/e2e_dup_${SUFFIX}' AS 'SELECT 2'" 2>&1 | grep -m1 -o 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT $SETTINGS -q "DROP HANDLER test_dup1_${SUFFIX}"

## Part 15: PUT and DELETE methods

$CLICKHOUSE_CLIENT $SETTINGS -q "CREATE HANDLER test_put_${SUFFIX} URL '/e2e_put_${SUFFIX}' METHODS (PUT) AS 'SELECT 600 AS put_result'"
RESULT_PUT=$($CLICKHOUSE_CURL -X PUT -d '' "${HTTP_BASE}/e2e_put_${SUFFIX}")
echo "put: ${RESULT_PUT}"
$CLICKHOUSE_CLIENT $SETTINGS -q "DROP HANDLER test_put_${SUFFIX}"

$CLICKHOUSE_CLIENT $SETTINGS -q "CREATE HANDLER test_del_${SUFFIX} URL '/e2e_del_${SUFFIX}' METHODS (DELETE) AS 'SELECT 700 AS delete_result'"
RESULT_DEL=$($CLICKHOUSE_CURL -X DELETE "${HTTP_BASE}/e2e_del_${SUFFIX}")
echo "delete: ${RESULT_DEL}"
$CLICKHOUSE_CLIENT $SETTINGS -q "DROP HANDLER test_del_${SUFFIX}"
