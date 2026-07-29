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

# 'Content-Disposition' is 19 characters long, so a name limit of 20 still lets the part headers
# through and bounds only the form field name itself.
NAME_LIMIT=20

USER_NAME="test_multipart_name_limit_user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER_NAME}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER_NAME} IDENTIFIED WITH no_password SETTINGS http_max_field_name_size = ${NAME_LIMIT}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.* TO ${USER_NAME}"

# A form field name of exactly 'http_max_field_name_size' characters is accepted: 'param_' (6) plus
# a 14-character parameter name.
${CLICKHOUSE_CURL} -sS -F "param_abcdefghijklmn=hello" \
    "${CLICKHOUSE_URL}&user=${USER_NAME}&query=SELECT+%7Babcdefghijklmn%3AString%7D"

# One character more is rejected.
${CLICKHOUSE_CURL} -sS -F "param_abcdefghijklmno=hello" \
    "${CLICKHOUSE_URL}&user=${USER_NAME}&query=SELECT+%7Babcdefghijklmno%3AString%7D" 2>&1 | \
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
