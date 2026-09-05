#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The multipart/form-data body is parsed after authentication, so the authenticated user's
# 'http_max_field_name_size' and 'http_max_fields' must bound the form fields of a multipart request
# as exactly as they bound the URL-encoded form parsing in 'readQuery'. The name of a multipart form
# field comes from the 'Content-Disposition' header of the part rather than from the request line,
# so it needs its own check, and the field counter must reject the (limit + 1)-th field.

USER_NAME="test_multipart_name_limit_user_${CLICKHOUSE_DATABASE}"

URL="${CLICKHOUSE_URL}&user=${USER_NAME}"

# 'http_max_field_name_size' bounds the names of the URL query parameters of the request as well, and
# the URL of a stateless test carries parameters with names of variable length, so the limit is
# derived from the request URL. 'Content-Disposition' is 19 characters long, so a limit of at least 20
# also lets the headers of a multipart part through and bounds only the form field name itself.
NAME_LIMIT=$(printf '%s' "${URL#*\?}" | tr '&' '\n' | sed 's/=.*//' | awk 'length($0) > max { max = length($0) } END { print max }')
if [[ ${NAME_LIMIT} -lt 20 ]]; then
    NAME_LIMIT=20
fi

# A form field is passed as 'param_<name>' (6 characters of prefix).
PARAM_NAME=$(printf 'a%.0s' $(seq 1 $(( NAME_LIMIT - 6 ))))

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER_NAME}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER_NAME} IDENTIFIED WITH no_password SETTINGS http_max_field_name_size = ${NAME_LIMIT}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.* TO ${USER_NAME}"

# A form field name of exactly 'http_max_field_name_size' characters is accepted.
${CLICKHOUSE_CURL} -sS -F "param_${PARAM_NAME}=hello" \
    "${URL}&query=SELECT+%7B${PARAM_NAME}%3AString%7D"

# One character more is rejected.
${CLICKHOUSE_CURL} -sS -F "param_${PARAM_NAME}a=hello" \
    "${URL}&query=SELECT+%7B${PARAM_NAME}a%3AString%7D" 2>&1 | \
    grep -o 'Field name too long' | head -n1

$CLICKHOUSE_CLIENT -q "DROP USER ${USER_NAME}"

USER_NAME="test_multipart_fields_limit_user_${CLICKHOUSE_DATABASE}"

URL="${CLICKHOUSE_URL}&user=${USER_NAME}&query=SELECT+1"

# 'http_max_fields' bounds the URL query parameters of the request as well, and the URL of a
# stateless test carries a variable number of them, so derive the limit from the request URL. The
# multipart body has its own field counter, so a limit equal to the number of URL parameters lets
# every one of them through and still bounds the body at the same number of form fields.
FIELDS_LIMIT=$(( $(printf '%s' "${URL#*\?}" | tr -cd '&' | wc -c) + 1 ))

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER_NAME}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER_NAME} IDENTIFIED WITH no_password SETTINGS http_max_fields = ${FIELDS_LIMIT}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.* TO ${USER_NAME}"

FIELDS=()
for i in $(seq 1 ${FIELDS_LIMIT}); do
    FIELDS+=(-F "param_p${i}=v")
done

# Exactly 'http_max_fields' multipart form fields are accepted.
${CLICKHOUSE_CURL} -sS "${FIELDS[@]}" "${URL}"

# One field more is rejected.
${CLICKHOUSE_CURL} -sS "${FIELDS[@]}" -F "param_extra=v" "${URL}" 2>&1 | \
    grep -o 'Too many form fields' | head -n1

$CLICKHOUSE_CLIENT -q "DROP USER ${USER_NAME}"
