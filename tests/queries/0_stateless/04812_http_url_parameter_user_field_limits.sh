#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The URL query string of an HTTP request is parsed before authentication, because the name of the
# user is one of its parameters, so at that point it can only be bounded by the server default
# settings. The parsed parameters are re-validated against the authenticated user's 'http_max_fields'
# and 'http_max_field_name_size' before the server acts on them, so that a user whose settings
# profile lowers these limits cannot have an over-limit query string processed.

USER_NAME="test_url_param_limits_user_${CLICKHOUSE_DATABASE}"

URL="${CLICKHOUSE_URL}&user=${USER_NAME}&query=SELECT+1"
QUERY_STRING="${URL#*\?}"

# The URL of a stateless test carries a variable number of parameters, with names of variable length,
# so both limits are derived from the request URL itself.
FIELDS_LIMIT=$(( $(printf '%s' "${QUERY_STRING}" | tr -cd '&' | wc -c) + 2 ))
NAME_LIMIT=$(printf '%s' "${QUERY_STRING}" | tr '&' '\n' | sed 's/=.*//' | awk 'length($0) > max { max = length($0) } END { print max }')
# A query parameter is passed as 'param_<name>', so the name limit has to leave room for the prefix.
if [[ ${NAME_LIMIT} -lt 8 ]]; then
    NAME_LIMIT=8
fi

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER_NAME}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER_NAME} IDENTIFIED WITH no_password SETTINGS http_max_fields = ${FIELDS_LIMIT}, http_max_field_name_size = ${NAME_LIMIT}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.* TO ${USER_NAME}"

PADDING=$(printf 'a%.0s' $(seq 1 $(( NAME_LIMIT - 6 ))))
# Two distinct parameter names of exactly 'http_max_field_name_size' characters, and one that is a
# single character longer.
PARAM_A="param_${PADDING}"
PARAM_B="param_b${PADDING%a}"
PARAM_TOO_LONG="param_${PADDING}a"

# A parameter name of exactly 'http_max_field_name_size' characters is accepted, one character more
# is rejected.
${CLICKHOUSE_CURL} -sS "${URL}&${PARAM_A}=v"
${CLICKHOUSE_CURL} -sS "${URL}&${PARAM_TOO_LONG}=v" 2>&1 | grep -o 'Field name too long' | head -n1

# The URL above carries 'http_max_fields - 1' parameters, so one more parameter is still accepted,
# while two more exceed the limit.
${CLICKHOUSE_CURL} -sS "${URL}&${PARAM_A}=v&${PARAM_B}=v" 2>&1 | grep -o 'Too many form fields' | head -n1

$CLICKHOUSE_CLIENT -q "DROP USER ${USER_NAME}"
