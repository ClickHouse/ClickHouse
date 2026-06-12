#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DEFAULT_MAX_NAME_SIZE=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.settings WHERE name='http_max_field_name_size'")
DEFAULT_MAX_VALUE_SIZE=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.settings WHERE name='http_max_field_value_size'")

python3 -c "print('a'*($DEFAULT_MAX_NAME_SIZE-2) + ';')" > $CLICKHOUSE_TMP/short_name.txt
python3 -c "print('a'*($DEFAULT_MAX_NAME_SIZE+1) + ';')" > $CLICKHOUSE_TMP/long_name.txt
python3 -c "print('a'*($DEFAULT_MAX_NAME_SIZE-2) + ': ' + 'b'*($DEFAULT_MAX_VALUE_SIZE-2))" > $CLICKHOUSE_TMP/short_short.txt
python3 -c "print('a'*($DEFAULT_MAX_NAME_SIZE-2) + ': ' + 'b'*($DEFAULT_MAX_VALUE_SIZE+1))" > $CLICKHOUSE_TMP/short_long.txt
python3 -c "print('a'*($DEFAULT_MAX_NAME_SIZE+1) + ': ' + 'b'*($DEFAULT_MAX_VALUE_SIZE-2))" > $CLICKHOUSE_TMP/long_short.txt

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -H @$CLICKHOUSE_TMP/short_name.txt -d 'SELECT 1'
${CLICKHOUSE_CURL} -sSv "${CLICKHOUSE_URL}" -H @$CLICKHOUSE_TMP/long_name.txt -d 'SELECT 1' 2>&1 | grep -Fc '400 Bad Request'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -H @$CLICKHOUSE_TMP/short_short.txt -d 'SELECT 1'
${CLICKHOUSE_CURL} -sSv "${CLICKHOUSE_URL}" -H @$CLICKHOUSE_TMP/short_long.txt -d 'SELECT 1' 2>&1 | grep -Fc '400 Bad Request'
${CLICKHOUSE_CURL} -sSv "${CLICKHOUSE_URL}" -H @$CLICKHOUSE_TMP/long_short.txt -d 'SELECT 1' 2>&1 | grep -Fc '400 Bad Request'

# Session and query settings shouldn't affect the HTTP field's name or value sizes.
${CLICKHOUSE_CURL} -sSv "${CLICKHOUSE_URL}&http_max_field_name_size=$(($DEFAULT_MAX_NAME_SIZE+10))" -H @$CLICKHOUSE_TMP/long_name.txt -d 'SELECT 1' 2>&1 | grep -Fc '400 Bad Request'
${CLICKHOUSE_CURL} -sSv "${CLICKHOUSE_URL}&http_max_field_value_size=$(($DEFAULT_MAX_VALUE_SIZE+10))" -H @$CLICKHOUSE_TMP/short_long.txt -d 'SELECT 1' 2>&1 | grep -Fc '400 Bad Request'
${CLICKHOUSE_CURL} -sSv "${CLICKHOUSE_URL}&http_max_field_name_size=$(($DEFAULT_MAX_NAME_SIZE+10))" -H @$CLICKHOUSE_TMP/long_short.txt -d 'SELECT 1' 2>&1 | grep -Fc '400 Bad Request'

# test that session context doesn't affect these settings either.
SESSION_ID="${CLICKHOUSE_DATABASE}_test_01903"

# Override the HTTP parsing limits inside the session. We use `SET` in the request body so the
# changes go through `InterpreterSetQuery::execute` and actually land in `session_context`,
# rather than being interpreted as URL parameter settings applied to the per-query context.
# Pass `-f` so a server-side failure of `SET` becomes a non-zero curl exit and visible error
# output, ensuring we are actually exercising a successful session-scoped override below.
${CLICKHOUSE_CURL} -sSf "${CLICKHOUSE_URL}&session_id=${SESSION_ID}" -d "SET http_max_field_name_size = $((DEFAULT_MAX_NAME_SIZE+10))"
${CLICKHOUSE_CURL} -sSf "${CLICKHOUSE_URL}&session_id=${SESSION_ID}" -d "SET http_max_field_value_size = $((DEFAULT_MAX_VALUE_SIZE+10))"
${CLICKHOUSE_CURL} -sSf "${CLICKHOUSE_URL}&session_id=${SESSION_ID}" -d 'SELECT 1'

# Test that the session context doesn't bypass the server's global limits for subsequent requests in the session.
${CLICKHOUSE_CURL} -sSv "${CLICKHOUSE_URL}&session_id=${SESSION_ID}" -H @$CLICKHOUSE_TMP/long_name.txt -d 'SELECT 1' 2>&1 | grep -Fc '400 Bad Request'
${CLICKHOUSE_CURL} -sSv "${CLICKHOUSE_URL}&session_id=${SESSION_ID}" -H @$CLICKHOUSE_TMP/short_long.txt -d 'SELECT 1' 2>&1 | grep -Fc '400 Bad Request'
${CLICKHOUSE_CURL} -sSv "${CLICKHOUSE_URL}&session_id=${SESSION_ID}" -H @$CLICKHOUSE_TMP/long_short.txt -d 'SELECT 1' 2>&1 | grep -Fc '400 Bad Request'
