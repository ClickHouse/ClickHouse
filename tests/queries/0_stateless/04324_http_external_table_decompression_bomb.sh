#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A single native-compressed block declares its decompressed size in the block header.
# That declared size is attacker-controlled, so it must be rejected *before* the
# decompressed buffer is allocated, otherwise a tiny crafted block could force a huge
# allocation. The cumulative decompressed-size limit checked inside CompressedReadBuffer
# rejects such a block up front, since its declared size alone already exceeds the limit.
#
# Here a single compressed block decompresses to ~1 MB (one full max_compress_block_size
# block), which is far larger than the small http_max_multipart_form_data_size below. The
# block must be rejected with TOO_LARGE_SIZE_COMPRESSED while reading its header, before any
# decompressed-block allocation happens.

USER_NAME="test_decompress_bomb_user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER_NAME}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER_NAME} IDENTIFIED WITH no_password SETTINGS http_max_multipart_form_data_size = 1000"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.* TO ${USER_NAME}"

# 1,000,000 UInt64 values => 8 MB of decompressed Native data => several full 1 MB blocks.
# The very first block declares ~1 MB decompressed, which exceeds the 1000 byte limit.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+number+AS+id+FROM+numbers(1000000)+FORMAT+Native&compress=1" | \
    ${CLICKHOUSE_CURL} -sSF 'ext=@-' "${CLICKHOUSE_URL}&user=${USER_NAME}&query=SELECT+count()+FROM+ext&ext_structure=id+UInt64&ext_format=Native&ext_decompress=1" 2>&1 | \
    grep -o 'TOO_LARGE_SIZE_COMPRESSED' | head -n1

$CLICKHOUSE_CLIENT -q "DROP USER ${USER_NAME}"
