#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The URL query string is validated against 'http_max_fields' of the session, and its field count
# is carried over to the parser of the request body. The request itself may lower the limit
# (through a per-request setting or a profile) before the body is parsed, so the carried-over
# count may already exceed the limit in effect. The body parser has to fail closed in that case
# instead of testing the count for equality with the limit, which would never hold again and
# would let the body add any number of fields.

USER_NAME="test_lowered_max_fields_user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER_NAME}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER_NAME} IDENTIFIED WITH no_password"

BASE_URL="${CLICKHOUSE_URL}&user=${USER_NAME}&query=SELECT+1"
QUERY_STRING="${BASE_URL#*\?}"

# The URL of a stateless test carries a variable number of parameters, so the limit is derived from
# the request URL itself. The parameters that are consumed before the user's settings are known are
# exempt from the limit; the 'http_max_fields' parameter added below counts.
EXEMPT_NAMES='^(user|password|quota_key|stacktrace|close_session|session_id|session_timeout|session_check)$'
URL_FIELDS=$(( $(printf '%s' "${QUERY_STRING}" | tr '&' '\n' | sed 's/=.*//' | grep -cvE "${EXEMPT_NAMES}") + 1 ))

# The request lowers the limit below the number of fields of its own URL.
LOWERED_URL="${BASE_URL}&http_max_fields=$(( URL_FIELDS - 1 ))"
# Control: a limit with room for exactly one body field.
ROOMY_URL="${BASE_URL}&http_max_fields=$(( URL_FIELDS + 1 ))"

# A body-less request is fine: the query string was accepted under the session's limit.
${CLICKHOUSE_CURL} -sS "${LOWERED_URL}"

# The body cannot add a field once the effective limit is already exceeded.
${CLICKHOUSE_CURL} -sS -F "param_a=v" "${LOWERED_URL}" 2>&1 | grep -o 'Too many form fields' | head -n1

# Control: with room left under the lowered limit, one body field is still accepted and two are not.
${CLICKHOUSE_CURL} -sS -F "param_a=v" "${ROOMY_URL}"
${CLICKHOUSE_CURL} -sS -F "param_a=v" -F "param_b=v" "${ROOMY_URL}" 2>&1 | grep -o 'Too many form fields' | head -n1

$CLICKHOUSE_CLIENT -q "DROP USER ${USER_NAME}"
