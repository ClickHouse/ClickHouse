#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The multipart/form-data parser buffers part content line by line to detect the boundary line
# that terminates the part, so a buffered line may legitimately hold the boundary line, which can
# be longer than 'http_max_multipart_form_data_size'. The multipart boundary is client-controlled,
# so that slack must not be granted to arbitrary content: bytes past the content limit are allowed
# only while the buffered line still matches the boundary line. Previously the whole boundary
# length was added to the content budget unconditionally, so a client could exceed the configured
# content limit by roughly the boundary size (up to 'http_max_request_header_size') just by
# choosing a huge boundary and omitting CRLF in the content.

LIMIT=100

USER_NAME="test_multipart_boundary_slack_user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER_NAME}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER_NAME} IDENTIFIED WITH no_password SETTINGS http_max_multipart_form_data_size = ${LIMIT}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.* TO ${USER_NAME}"

URL="${CLICKHOUSE_URL}&user=${USER_NAME}&query=SELECT+length(s)+FROM+ext&ext_structure=s+String&ext_format=TSV"

# A boundary much longer than the content limit.
BOUNDARY=$(yes b 2>/dev/null | tr -d '\n' | head -c 2000)

send_multipart_with_content_size()
{
    {
        printf -- '--%s\r\n' "${BOUNDARY}"
        printf 'Content-Disposition: form-data; name="ext"; filename="data"\r\n\r\n'
        yes x 2>/dev/null | tr -d '\n' | head -c "$1"
        printf -- '\r\n--%s--\r\n' "${BOUNDARY}"
    } | ${CLICKHOUSE_CURL} -sS -X POST -H "Content-Type: multipart/form-data; boundary=${BOUNDARY}" --data-binary @- "${URL}"
}

# CRLF-free content of exactly the limit size is accepted: the huge boundary line that terminates
# it is allowed to outgrow the content limit because it matches the boundary.
send_multipart_with_content_size ${LIMIT}

# CRLF-free content larger than the limit is rejected as soon as the buffered line outgrows the
# limit and stops matching the boundary line: the boundary length must not be granted to the
# content as extra budget. The content here is well below limit + boundary size, which was
# accepted before.
send_multipart_with_content_size 500 2>&1 | grep -o 'LIMIT_EXCEEDED' | head -n1

$CLICKHOUSE_CLIENT -q "DROP USER ${USER_NAME}"
