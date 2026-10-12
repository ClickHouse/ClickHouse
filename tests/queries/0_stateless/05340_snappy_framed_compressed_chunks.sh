#!/usr/bin/env bash
# Tags: no-fasttest
#
# Test: SnappyFramedReadBuffer correctly processes CHUNK_TYPE_COMPRESSED (0x00) chunks.
# SnappyFramedWriteBuffer writes compressed chunks when snappy compression reduces the
# payload size (writeCompressedChunk: if compressed_size < size, use CHUNK_TYPE_COMPRESSED).
# Existing CI tests (04201) only write small integer sequences that do not compress, so
# they always produce CHUNK_TYPE_UNCOMPRESSED chunks. This test uses large repetitive
# string data which compresses well, forcing the writer to emit CHUNK_TYPE_COMPRESSED
# chunks and exercising the previously untested compressed-chunk reading path in
# SnappyFramedReadBuffer::nextImpl (src/IO/SnappyFramedReadBuffer.cpp lines 150-191):
# GetUncompressedLength, uncompressed-length bounds check, RawUncompress, CRC-32C
# verification, and working_buffer assignment.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${DIR}"
chmod 777 "${DIR}"
trap 'rm -rf "${DIR}"' EXIT

FILE="${DIR}/compressed_chunks.snappy"

# Write three rows of a 16000-char repeating string. The ~48 KB payload compresses
# to a small fraction of its size under snappy, so SnappyFramedWriteBuffer picks
# CHUNK_TYPE_COMPRESSED for the data chunk.
${CLICKHOUSE_CLIENT} -q "
INSERT INTO TABLE FUNCTION file('${FILE}', 'CSV', 'x String', 'snappy')
SELECT repeat('aaabbbcccdddeee_', 1000)
FROM numbers(3)
SETTINGS snappy_mode = 'framed';
"

# Read back via SnappyFramedReadBuffer with snappy_mode='framed'.
# This exercises the compressed-chunk path (lines 150-191).
${CLICKHOUSE_CLIENT} -q "
SELECT length(x), leftUTF8(x, 16)
FROM file('${FILE}', 'CSV', 'x String', 'snappy')
ORDER BY rowNumberInAllBlocks()
SETTINGS snappy_mode = 'framed';
"
