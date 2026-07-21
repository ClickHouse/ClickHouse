#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The multipart/form-data request body is parsed after authentication, so the form-data limits of
# the authenticated user's settings profile must be applied to it. Previously only
# 'http_max_multipart_form_data_size' was re-applied after authentication, while 'http_max_fields',
# 'http_max_field_name_size' and 'http_max_field_value_size' kept the server default values, so a
# user with stricter per-profile limits could submit more fields or longer names/values than their
# settings allow simply by using a multipart request.

LIMIT=256

USER_NAME="test_multipart_field_limits_user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER_NAME}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER_NAME} IDENTIFIED WITH no_password SETTINGS http_max_field_value_size = ${LIMIT}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.* TO ${USER_NAME}"

URL="${CLICKHOUSE_URL}&user=${USER_NAME}&query=SELECT+length(%7Bp1%3AString%7D)"

# A multipart form field within the user's limit is accepted.
${CLICKHOUSE_CURL} -sS -F "param_p1=$(yes x 2>/dev/null | tr -d '\n' | head -c ${LIMIT})" "${URL}"

# A multipart form field longer than the user's 'http_max_field_value_size' (but well under the
# server default) is rejected.
${CLICKHOUSE_CURL} -sS -F "param_p1=$(yes x 2>/dev/null | tr -d '\n' | head -c 1000)" "${URL}" 2>&1 | \
    grep -o 'Field value too long' | head -n1

$CLICKHOUSE_CLIENT -q "DROP USER ${USER_NAME}"
