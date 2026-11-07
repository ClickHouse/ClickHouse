#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The multipart/form-data parser reads part content line by line to detect boundary lines, and a
# line is buffered in memory in full. Content with no CRLF must therefore be rejected as soon as
# the buffered line outgrows 'http_max_multipart_form_data_size', instead of being accumulated
# until the next CRLF (which an attacker can simply omit). In particular, a request whose first
# 'http_max_multipart_form_data_size' bytes form a valid external table followed by a huge
# CRLF-free tail must not make the server buffer the tail while it probes for data past the limit.

LIMIT=1000

USER_NAME="test_multipart_line_limit_user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER_NAME}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER_NAME} IDENTIFIED WITH no_password SETTINGS http_max_multipart_form_data_size = ${LIMIT}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.* TO ${USER_NAME}"

URL="${CLICKHOUSE_URL}&user=${USER_NAME}&query=SELECT+length(s)+FROM+ext&ext_structure=s+String&ext_format=TSV"

# A CRLF-free part of exactly the limit size is accepted.
yes x 2>/dev/null | tr -d '\n' | head -c ${LIMIT} | ${CLICKHOUSE_CURL} -sSF 'ext=@-' "${URL}"

# A CRLF-free part larger than the limit is rejected when the buffered line outgrows the limit,
# not after the whole content has been read into memory.
yes x 2>/dev/null | tr -d '\n' | head -c $((5 * 1000 * 1000)) | ${CLICKHOUSE_CURL} -sSF 'ext=@-' "${URL}" 2>&1 | \
    grep -o 'LIMIT_EXCEEDED' | head -n1

# Valid content filling the limit exactly, followed by a huge CRLF-free tail. The over-limit
# data is detected while probing for data past the limit, and the probe must reject the tail
# as soon as it outgrows the limit instead of buffering it in full.
{
    yes x 2>/dev/null | tr -d '\n' | head -c ${LIMIT}
    printf '\r\n'
    yes y 2>/dev/null | tr -d '\n' | head -c $((5 * 1000 * 1000))
} | ${CLICKHOUSE_CURL} -sSF 'ext=@-' "${URL}" 2>&1 | \
    grep -o 'LIMIT_EXCEEDED' | head -n1

$CLICKHOUSE_CLIENT -q "DROP USER ${USER_NAME}"
