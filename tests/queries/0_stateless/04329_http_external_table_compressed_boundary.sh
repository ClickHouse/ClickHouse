#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Exact-boundary regression for the COMPRESSED-byte multipart limit on compressed external HTTP
# data. The decompressed-size limit is enforced cumulatively inside CompressedReadBuffer, but the
# raw compressed bytes are still bounded by an outer LimitReadBuffer of
# http_max_multipart_form_data_size. That buffer must reject input exceeding the cap instead of
# reporting EOF at the exact boundary: otherwise a stream whose compressed bytes end precisely on
# the limit with more compressed blocks appended would load a truncated external table and succeed,
# producing incorrect query results.
#
# Incompressible data is used so that one compressed block is at least as large as its decompressed
# size; the limit is set to the compressed size of one block, so the compressed-byte cap (not the
# cumulative decompressed check) is what binds at the boundary.

DATA_FILE="${CLICKHOUSE_TMP}/04329_block.native"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+randomString(50)+AS+s+FROM+numbers(100)+FORMAT+Native&compress=1" > "$DATA_FILE"
COMPRESSED_SIZE=$(wc -c < "$DATA_FILE")

# User names are server-global, so scope the name to the test database to avoid collisions when
# the test runs concurrently (e.g. in the flaky check).
USER_NAME="test_compressed_boundary_user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER_NAME}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER_NAME} IDENTIFIED WITH no_password SETTINGS http_max_multipart_form_data_size = ${COMPRESSED_SIZE}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.* TO ${USER_NAME}"

# One compressed block exactly at the limit: accepted, returns 100.
${CLICKHOUSE_CURL} -sSF 'ext=@'"$DATA_FILE" "${CLICKHOUSE_URL}&user=${USER_NAME}&query=SELECT+count()+FROM+ext&ext_structure=s+String&ext_format=Native&ext_decompress=1"

# Two such blocks: the first fills the compressed budget exactly, so the second pushes the
# compressed bytes past the cap and must be rejected instead of being silently truncated at EOF.
cat "$DATA_FILE" "$DATA_FILE" | \
    ${CLICKHOUSE_CURL} -sSF 'ext=@-' "${CLICKHOUSE_URL}&user=${USER_NAME}&query=SELECT+count()+FROM+ext&ext_structure=s+String&ext_format=Native&ext_decompress=1" 2>&1 | \
    grep -o -m1 'LIMIT_EXCEEDED'

$CLICKHOUSE_CLIENT -q "DROP USER ${USER_NAME}"
rm -f "$DATA_FILE"
