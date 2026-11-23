#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# 'http_max_fields' bounds the number of form fields of the request as a whole. The fields arrive on
# two different carriers - the URL query string, which is parsed before authentication, and the
# request body, which is parsed after it - so the count has to be carried from the first to the
# second; otherwise the limit could be exceeded by sending it once on each carrier.

USER_NAME="test_mixed_carrier_fields_user_${CLICKHOUSE_DATABASE}"

URL="${CLICKHOUSE_URL}&user=${USER_NAME}&query=SELECT+1"
QUERY_STRING="${URL#*\?}"

# The URL of a stateless test carries a variable number of parameters, with names of variable length,
# so the limits are derived from the request URL itself. The parameters that are consumed before the
# user's settings are known are exempt from the re-validation and do not count towards the limit.
EXEMPT_NAMES='^(user|password|quota_key|stacktrace|close_session|session_id|session_timeout|session_check)$'
URL_FIELDS=$(printf '%s' "${QUERY_STRING}" | tr '&' '\n' | sed 's/=.*//' | grep -cvE "${EXEMPT_NAMES}")
# The URL alone stays one field below the limit, so exactly one body field still fits.
FIELDS_LIMIT=$(( URL_FIELDS + 1 ))
# 'http_max_field_name_size' also bounds the header names of a multipart body, and
# 'Content-Disposition' is 19 characters long, so the name limit has to stay above that.
NAME_LIMIT=$(printf '%s' "${QUERY_STRING}" | tr '&' '\n' | sed 's/=.*//' | awk 'length($0) > max { max = length($0) } END { if (max < 20) max = 20; print max }')
VALUE_LIMIT=$(printf '%s' "${QUERY_STRING}" | tr '&' '\n' | sed 's/^[^=]*=//' | awk 'length($0) > max { max = length($0) } END { if (max < 16) max = 16; print max }')

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER_NAME}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER_NAME} IDENTIFIED WITH no_password SETTINGS http_max_fields = ${FIELDS_LIMIT}, http_max_field_name_size = ${NAME_LIMIT}, http_max_field_value_size = ${VALUE_LIMIT}"

# A body-less request does not consume a field slot of its own: the URL is below the limit, so it passes.
${CLICKHOUSE_CURL} -sS "${URL}"

# One multipart field brings the total to exactly 'http_max_fields' - accepted.
${CLICKHOUSE_CURL} -sS -F "param_a=v" "${URL}"

# Two multipart fields exceed the limit, even though the body alone is well below it.
${CLICKHOUSE_CURL} -sS -F "param_a=v" -F "param_b=v" "${URL}" 2>&1 | grep -o 'Too many form fields' | head -n1

# The same holds for an external table, which is uploaded as a multipart field as well: a request
# whose URL is already at the limit cannot smuggle extra fields through the body.
${CLICKHOUSE_CURL} -sS -F "t=1" -F "t_structure=x UInt8" -F "t_format=TSV" "${URL}" 2>&1 | grep -o 'Too many form fields' | head -n1

$CLICKHOUSE_CLIENT -q "DROP USER ${USER_NAME}"
