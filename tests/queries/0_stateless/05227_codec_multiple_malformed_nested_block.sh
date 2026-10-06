#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Malformed blocks of the `Multiple` codec have to be rejected before anything is decoded into the
# destination buffer, which the caller sizes from the outer header only.
#
# The layout of a block is: 16 bytes of the checksum (not verified here), 1 byte of the method
# (0x91 is `Multiple`), 4 bytes of the compressed size including the 9 bytes of the header, 4 bytes
# of the uncompressed size, then the number of nested codecs, one method byte per codec and the
# nested block. The numbers are little endian.

URL="${CLICKHOUSE_URL}&decompress=1&http_native_compression_disable_checksumming_on_decompress=1"

# 127 nested codecs declared in a 3-byte payload.
echo -ne '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x91\x0c\x00\x00\x00\x01\x00\x00\x00\x7f\x00\x00' |
    ${CLICKHOUSE_CURL} -sS "$URL" --data-binary @- 2>&1 |
    grep -oF 'Wrong compression methods list: header claims 127 codecs but compressed data is only 3 bytes'

# One nested codec (0x02 is NONE) followed by 4 bytes, less than a nested block header.
echo -ne '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x91\x0f\x00\x00\x00\x01\x00\x00\x00\x01\x02\x00\x00\x00\x00' |
    ${CLICKHOUSE_CURL} -sS "$URL" --data-binary @- 2>&1 |
    grep -oF 'Compressed data is too short to contain a block header: 4 bytes'

# The nested block of the last stage claims 16 uncompressed bytes while the outer header claims 10.
echo -ne '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x91\x24\x00\x00\x00\x0a\x00\x00\x00\x01\x02\x02\x19\x00\x00\x00\x10\x00\x00\x00\x41\x41\x41\x41\x41\x41\x41\x41\x41\x41\x41\x41\x41\x41\x41\x41' |
    ${CLICKHOUSE_CURL} -sS "$URL" --data-binary @- 2>&1 |
    grep -oF 'Wrong final decompressed size in codec Multiple, got 16, expected 10'

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "SELECT 'Ok.'"
