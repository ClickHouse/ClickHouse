#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A compressed block with an empty body: the compressed size is exactly the size of the header, while
# the uncompressed size is not zero. The decompressor has nothing to read from, so it must fail
# instead of reporting success and handing out the previous contents of the destination buffer.
#
# The layout of the block is: 16 bytes of the checksum (not verified here), 1 byte of the method
# (0x82 is LZ4), 4 bytes of the compressed size including the 9 bytes of the header, and 4 bytes of
# the uncompressed size. The numbers are little endian.

echo -ne '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x82\x09\x00\x00\x00\x10\x00\x00\x00' |
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&decompress=1&http_native_compression_disable_checksumming_on_decompress=1" --data-binary @- 2>&1 |
    grep -oF 'Cannot decompress LZ4-encoded data'

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "SELECT 'Ok.'"
