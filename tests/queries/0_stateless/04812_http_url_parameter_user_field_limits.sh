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
# so all the limits are derived from the request URL itself. The parameters that are consumed before
# the user's settings are known are exempt from the re-validation, so they do not count towards the
# limit on the number of fields either.
EXEMPT_NAMES='^(user|password|quota_key|stacktrace|close_session|session_id|session_timeout|session_check)$'
FIELDS_LIMIT=$(( $(printf '%s' "${QUERY_STRING}" | tr '&' '\n' | sed 's/=.*//' | grep -cvE "${EXEMPT_NAMES}") + 1 ))
NAME_LIMIT=$(printf '%s' "${QUERY_STRING}" | tr '&' '\n' | sed 's/=.*//' | awk 'length($0) > max { max = length($0) } END { print max }')
VALUE_LIMIT=$(printf '%s' "${QUERY_STRING}" | tr '&' '\n' | sed 's/^[^=]*=//' | awk 'length($0) > max { max = length($0) } END { print max }')
# The name limit also bounds the header names of a multipart body, and 'Content-Disposition' is 19
# characters long, so keep it above that; a query parameter is passed as 'param_<name>', so the
# padding below has to leave room for the prefix as well.
if [[ ${NAME_LIMIT} -lt 20 ]]; then
    NAME_LIMIT=20
fi

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER_NAME}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER_NAME} IDENTIFIED WITH no_password SETTINGS http_max_fields = ${FIELDS_LIMIT}, http_max_field_name_size = ${NAME_LIMIT}, http_max_field_value_size = ${VALUE_LIMIT}"
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

# The URL above carries 'http_max_fields - 1' parameters that are subject to the check, so one more
# parameter is still accepted, while two more exceed the limit.
${CLICKHOUSE_CURL} -sS "${URL}&${PARAM_A}=v&${PARAM_B}=v" 2>&1 | grep -o 'Too many form fields' | head -n1

# Authentication and named-session selectors are consumed before the user's settings are available,
# so they stay bounded by the server defaults. A selector longer than this user's value limit must
# not fail later after the named session has already been acquired.
SESSION_ID=$(printf 's%.0s' $(seq 1 $(( VALUE_LIMIT + 1 ))))
${CLICKHOUSE_CURL} -sS "${URL}&session_id=${SESSION_ID}&close_session=1"

# The same holds for a multipart request: parsing its body must not re-parse the URL query string
# under the authenticated user's limits, otherwise the request is rejected after the named session
# has already been acquired.
${CLICKHOUSE_CURL} -sS -F "param_p1=v" "${URL}&session_id=${SESSION_ID}m&close_session=1"

$CLICKHOUSE_CLIENT -q "DROP USER ${USER_NAME}"
