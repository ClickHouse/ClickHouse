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

# Prove that the writer really emitted a compressed chunk, so the read below exercises the
# compressed path and not the uncompressed fallback that SnappyFramedWriteBuffer takes when
# compressed_size >= size. A framed stream starts with the 10-byte stream identifier chunk
# (type 0xff, length 6, "sNaPpY"); every chunk after it has a 1-byte type and a 3-byte
# little-endian length. Type 0x00 is a compressed data chunk, type 0x01 an uncompressed one.
compressed_chunks=0
uncompressed_chunks=0
offset=10
size=$(stat -c %s "${FILE}")
while [ "${offset}" -lt "${size}" ]; do
    chunk_type=$(od -An -tx1 -j "${offset}" -N 1 "${FILE}" | tr -d ' \n')
    read -r b0 b1 b2 < <(od -An -tu1 -j $((offset + 1)) -N 3 "${FILE}")
    chunk_len=$((b0 + b1 * 256 + b2 * 65536))
    case "${chunk_type}" in
        00) compressed_chunks=$((compressed_chunks + 1)) ;;
        01) uncompressed_chunks=$((uncompressed_chunks + 1)) ;;
    esac
    offset=$((offset + 4 + chunk_len))
done
echo "compressed chunks present: $([ "${compressed_chunks}" -gt 0 ] && echo 1 || echo 0)"
echo "uncompressed chunks present: $([ "${uncompressed_chunks}" -gt 0 ] && echo 1 || echo 0)"

# Read back via SnappyFramedReadBuffer with snappy_mode='framed'.
# This exercises the compressed-chunk path (lines 150-191).
${CLICKHOUSE_CLIENT} -q "
SELECT length(x), leftUTF8(x, 16)
FROM file('${FILE}', 'CSV', 'x String', 'snappy')
ORDER BY rowNumberInAllBlocks()
SETTINGS snappy_mode = 'framed';
"
