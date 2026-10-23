#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The boundary and header lines of a multipart/form-data body are bounded by
# 'http_max_request_header_size'. The body is parsed after authentication, so the limit has to be
# the authenticated user's one, like the other body-parsing limits: previously the value captured
# from the server defaults before authentication was kept, so a user whose profile lowers the
# limit could still send boundary or 'Content-Disposition' lines up to the server default.

LIMIT=200

USER_NAME="test_multipart_header_size_user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER_NAME}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER_NAME} IDENTIFIED WITH no_password SETTINGS http_max_request_header_size = ${LIMIT}"

URL="${CLICKHOUSE_URL}&user=${USER_NAME}&query=SELECT+length(s)+FROM+ext&ext_structure=s+String&ext_format=TSV"

# Sends a single-part body with the given boundary and the given field name (the name is part of
# the 'Content-Disposition' header line of the part).
send_multipart()
{
    local boundary="$1"
    local name="$2"
    {
        printf -- '--%s\r\n' "${boundary}"
        printf 'Content-Disposition: form-data; name="%s"; filename="data"\r\n\r\n' "${name}"
        printf 'xyz'
        printf -- '\r\n--%s--\r\n' "${boundary}"
    } | ${CLICKHOUSE_CURL} -sS -X POST -H "Content-Type: multipart/form-data; boundary=${boundary}" --data-binary @- "${URL}"
}

SHORT_BOUNDARY=$(yes b 2>/dev/null | tr -d '\n' | head -c 40)
LONG_BOUNDARY=$(yes b 2>/dev/null | tr -d '\n' | head -c 300)
LONG_NAME=$(yes n 2>/dev/null | tr -d '\n' | head -c 300)

# Lines within the user's limit are accepted.
send_multipart "${SHORT_BOUNDARY}" "ext"

# A boundary line longer than the user's limit is rejected, although it is far below the server default.
send_multipart "${LONG_BOUNDARY}" "ext" 2>&1 | grep -o "tuned by the 'http_max_request_header_size' setting" | head -n1

# So is a 'Content-Disposition' header line longer than the user's limit. The name fits
# 'http_max_field_name_size', so it is the size of the line that rejects it.
send_multipart "${SHORT_BOUNDARY}" "${LONG_NAME}" 2>&1 | grep -o "tuned by the 'http_max_request_header_size' setting" | head -n1

$CLICKHOUSE_CLIENT -q "DROP USER ${USER_NAME}"
