#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A buffered multipart/form-data content line may outgrow 'http_max_multipart_form_data_size'
# only while it is a valid delimiter line: the boundary, optionally followed by the "--" of the
# close delimiter and by linear whitespace (RFC 2046 transport padding). Arbitrary bytes after the
# boundary must not be accepted past the content limit, because the whole line is discarded as a
# boundary line, so the client could smuggle bytes past the limit and truncate the payload.

LIMIT=100

USER_NAME="test_multipart_boundary_suffix_user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER_NAME}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER_NAME} IDENTIFIED WITH no_password SETTINGS http_max_multipart_form_data_size = ${LIMIT}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.* TO ${USER_NAME}"

URL="${CLICKHOUSE_URL}&user=${USER_NAME}&query=SELECT+length(s)+FROM+ext&ext_structure=s+String&ext_format=TSV"

BOUNDARY="bnd"

# $1 - the bytes that follow the closing boundary on its line.
send_multipart_with_delimiter_suffix()
{
    {
        printf -- '--%s\r\n' "${BOUNDARY}"
        printf 'Content-Disposition: form-data; name="ext"; filename="data"\r\n\r\n'
        yes x 2>/dev/null | tr -d '\n' | head -c 50
        printf -- '\r\n--%s%s\r\n' "${BOUNDARY}" "$1"
    } | ${CLICKHOUSE_CURL} -sS -X POST -H "Content-Type: multipart/form-data; boundary=${BOUNDARY}" --data-binary @- "${URL}"
}

PADDING=$(printf '%200s' '')
GARBAGE=$(yes y 2>/dev/null | tr -d '\n' | head -c 200)

# A close delimiter with transport padding longer than the content limit is accepted.
send_multipart_with_delimiter_suffix "--${PADDING}"

# Arbitrary bytes after the boundary past the content limit are rejected.
send_multipart_with_delimiter_suffix "--${GARBAGE}--" 2>&1 | grep -o 'LIMIT_EXCEEDED' | head -n1
send_multipart_with_delimiter_suffix "${PADDING}${GARBAGE}--" 2>&1 | grep -o 'LIMIT_EXCEEDED' | head -n1

$CLICKHOUSE_CLIENT -q "DROP USER ${USER_NAME}"
