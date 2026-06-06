#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test that http_max_multipart_form_data_size is enforced on the *total* decompressed data,
# across several compressed blocks. Each individual block here decompresses to about 1 MB
# (one max_compress_block_size block), which is below the limit, so the per-block guard does
# not fire; instead the cumulative decompressed size exceeds the limit, the outer
# LimitReadBuffer returns EOF mid-stream, and the format reader throws CANNOT_READ_ALL_DATA.
# (The single-oversized-block path is covered separately in
# 04324_http_external_table_decompression_bomb.)

# User names are server-global, so scope the name to the test database to avoid
# collisions when the test runs concurrently (e.g. in the flaky check).
USER_NAME="test_decompress_limit_user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER_NAME}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER_NAME} IDENTIFIED WITH no_password SETTINGS http_max_multipart_form_data_size = 1500000"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.* TO ${USER_NAME}"

# 1,000,000 constant UInt64 values => 8 MB of decompressed Native data in ~1 MB blocks, but
# a tiny compressed payload (so the compressed-stream limit is not reached first). The 1.5 MB
# limit is larger than a single block but smaller than the total, so it is the cumulative
# decompressed size that trips the limit.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+CAST(0,'UInt64')+AS+id+FROM+numbers(1000000)+FORMAT+Native&compress=1" | \
    ${CLICKHOUSE_CURL} -sSF 'ext=@-' "${CLICKHOUSE_URL}&user=${USER_NAME}&query=SELECT+count()+FROM+ext&ext_structure=id+UInt64&ext_format=Native&ext_decompress=1" 2>&1 | \
    grep -o 'CANNOT_READ_ALL_DATA'

$CLICKHOUSE_CLIENT -q "DROP USER ${USER_NAME}"
