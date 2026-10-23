#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Exact-boundary regression for the decompressed-size limit on compressed external HTTP data.
#
# A complete, valid compressed block whose decompressed size is exactly the limit must be
# accepted, but if more compressed blocks follow they must be rejected with an error
# (TOO_LARGE_SIZE_COMPRESSED) rather than silently truncated. Previously the limit was enforced
# by an outer LimitReadBuffer that reported EOF at the cap, so an attacker could place a
# complete format stream exactly at the limit and append more blocks; the request would then
# load a truncated external table and succeed, producing incorrect query results. The limit is
# now enforced cumulatively inside CompressedReadBuffer, before each block is allocated.

# Decompressed size of one Native block of 100 UInt64 values (no compression), used as the
# exact limit so the first block lands precisely on the boundary. Zero values are used so
# that the compressed payload stays far below the limit: the multipart parser independently
# rejects a part whose raw content outgrows the limit, and this test exercises the
# cumulative decompressed-size check specifically.
BLOCK_SIZE=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+toUInt64(0)+AS+id+FROM+numbers(100)+FORMAT+Native" | wc -c)

# User names are server-global, so scope the name to the test database to avoid
# collisions when the test runs concurrently (e.g. in the flaky check).
USER_NAME="test_decompress_boundary_user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER_NAME}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER_NAME} IDENTIFIED WITH no_password SETTINGS http_max_multipart_form_data_size = ${BLOCK_SIZE}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.* TO ${USER_NAME}"

# One block exactly at the limit: accepted, returns 100.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+toUInt64(0)+AS+id+FROM+numbers(100)+FORMAT+Native&compress=1" | \
    ${CLICKHOUSE_CURL} -sSF 'ext=@-' "${CLICKHOUSE_URL}&user=${USER_NAME}&query=SELECT+count()+FROM+ext&ext_structure=id+UInt64&ext_format=Native&ext_decompress=1"

# Two such blocks: the first fills the budget exactly, so the second must be rejected before
# allocation instead of being silently dropped at an artificial EOF.
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+toUInt64(0)+AS+id+FROM+numbers(100)+FORMAT+Native&compress=1"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+toUInt64(0)+AS+id+FROM+numbers(100)+FORMAT+Native&compress=1"
} | \
    ${CLICKHOUSE_CURL} -sSF 'ext=@-' "${CLICKHOUSE_URL}&user=${USER_NAME}&query=SELECT+count()+FROM+ext&ext_structure=id+UInt64&ext_format=Native&ext_decompress=1" 2>&1 | \
    grep -o 'TOO_LARGE_SIZE_COMPRESSED' | head -n1

$CLICKHOUSE_CLIENT -q "DROP USER ${USER_NAME}"
