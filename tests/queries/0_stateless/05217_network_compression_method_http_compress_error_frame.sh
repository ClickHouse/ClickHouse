#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The body of an error response requested with `compress=1` is framed the same way as a successful
# response: a native-protocol compressed frame whose codec comes from `network_compression_method`.
# `HTTPHandler::trySendExceptionToClient` writes the exception into the same `CompressedWriteBuffer`
# that `processQuery` created for the query output, so a `compress=1` client can decode the error
# with the codec it asked for instead of failing on the frame.
# Byte 17 of the frame, right after the 16-byte checksum, is the compression method byte of
# `CompressionMethodByte`: 0x82 for LZ4, 0x90 for ZSTD.
error_frame()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&compress=1&network_compression_method=$1" -d 'SELECT throwIf(1)'
}

for method in LZ4 ZSTD
do
    error_frame "$method" | tail -c +17 | head -c 1 | od -An -tx1 | tr -d ' \n'
    echo
    error_frame "$method" | ${CLICKHOUSE_COMPRESSOR} --decompress | grep -o 'FUNCTION_THROW_IF_VALUE_IS_NON_ZERO'
done
