#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A response requested with `compress=1` is framed exactly like the native protocol's compressed
# packets, so its codec comes from `network_compression_method` rather than from the default codec
# for table data. Byte 17 of the frame, right after the 16-byte checksum, is the compression method
# byte of `CompressionMethodByte`: 0x82 for LZ4 and LZ4HC, 0x90 for ZSTD, 0x02 for NONE.
method_byte()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&compress=1&network_compression_method=$1" -d 'SELECT 1' \
        | tail -c +17 | head -c 1 | od -An -tx1 | tr -d ' \n'
    echo
}

method_byte LZ4
method_byte LZ4HC
method_byte ZSTD
method_byte NONE

# A codec that is not usable on the wire is rejected instead of being ignored, as over the native protocol.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&compress=1&network_compression_method=T64" -d 'SELECT 1' \
    | grep -o 'must be NONE, ZSTD, LZ4 or LZ4HC'
