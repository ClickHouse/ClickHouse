#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `http_max_multipart_form_data_size` limits the *content* of a multipart/form-data part, not the
# request syntax. With a tiny limit, the multipart boundary line and the `Content-Disposition`
# header are necessarily longer than the limit, but they belong to the request syntax (bounded by
# `http_max_request_header_size`) and must not be rejected. Previously the same small limit was
# applied to every buffered line, so a valid tiny upload failed before the part was handled.

LIMIT=8

USER_NAME="test_multipart_tiny_limit_user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER_NAME}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER_NAME} IDENTIFIED WITH no_password SETTINGS http_max_multipart_form_data_size = ${LIMIT}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.* TO ${USER_NAME}"

URL="${CLICKHOUSE_URL}&user=${USER_NAME}&query=SELECT+sum(id)+FROM+ext&ext_structure=id+UInt8&ext_format=TSV"

# A tiny valid upload (content well under the limit) whose multipart boundary and
# Content-Disposition header lines are far longer than ${LIMIT}. It must be accepted and return
# the sum (1 + 2 + 3 = 6).
printf '1\n2\n3\n' | ${CLICKHOUSE_CURL} -sSF 'ext=@-' "${URL}"

$CLICKHOUSE_CLIENT -q "DROP USER ${USER_NAME}"
