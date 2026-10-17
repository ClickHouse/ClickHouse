#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A line inside the content of a multipart/form-data part that starts with "\r\n--<boundary>"
# terminates the part: it is request syntax rather than content, so it must be bounded by the
# user's 'http_max_request_header_size' while it is buffered, even though it is far below the
# content limit ('http_max_multipart_form_data_size'). Ordinary content lines longer than
# 'http_max_request_header_size' are still accepted, including the first line of a part's content,
# which the header parser reads ahead while it looks for the empty line that ends the headers.

LIMIT=200

USER_NAME="test_multipart_content_boundary_user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER_NAME}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER_NAME} IDENTIFIED WITH no_password SETTINGS http_max_request_header_size = ${LIMIT}"

URL="${CLICKHOUSE_URL}&user=${USER_NAME}&query=SELECT+length(s)+FROM+ext&ext_structure=s+String&ext_format=TSV"

BOUNDARY=$(yes b 2>/dev/null | tr -d '\n' | head -c 40)
LONG_TAIL=$(yes x 2>/dev/null | tr -d '\n' | head -c 300)

# Sends a single-part body whose content is followed by the given text right before the final boundary.
send_multipart()
{
    local tail="$1"
    {
        printf -- '--%s\r\n' "${BOUNDARY}"
        printf 'Content-Disposition: form-data; name="ext"; filename="data"\r\n\r\n'
        printf 'xyz'
        printf '%s' "${tail}"
        printf -- '\r\n--%s--\r\n' "${BOUNDARY}"
    } | ${CLICKHOUSE_CURL} -sS -X POST -H "Content-Type: multipart/form-data; boundary=${BOUNDARY}" --data-binary @- "${URL}"
}

# A content line longer than the user's limit is content, not syntax, so it is accepted.
send_multipart "${LONG_TAIL}"

# A boundary line in the content followed by a long tail without CRLF is rejected by the user's limit.
send_multipart "$(printf -- '\r\n--%s%s' "${BOUNDARY}" "${LONG_TAIL}")" 2>&1 | grep -o "tuned by the 'http_max_request_header_size' setting" | head -n1

$CLICKHOUSE_CLIENT -q "DROP USER ${USER_NAME}"
