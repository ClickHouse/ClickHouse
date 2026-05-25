#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Build a framed snappy stream with many zero-length chunks, then a real query
# payload. `ReadBuffer::next` recursively calls itself when `nextImpl` returns
# `true` with an empty `working_buffer`, so a crafted stream of empty chunks
# could exhaust the stack. Verify that `SnappyFramedReadBuffer` skips empty
# chunks at the source and decodes the trailing real chunk correctly.

PAYLOAD_FILE="${CLICKHOUSE_TMP}/snappy_empty_chunks_payload.bin"

python3 - "$PAYLOAD_FILE" <<'PY'
import struct
import sys

CRC_TABLE = []
for i in range(256):
    c = i
    for _ in range(8):
        c = (c >> 1) ^ (0x82F63B78 if c & 1 else 0)
    CRC_TABLE.append(c)

def crc32c(data):
    c = 0xFFFFFFFF
    for b in data:
        c = (c >> 8) ^ CRC_TABLE[(c ^ b) & 0xFF]
    return c ^ 0xFFFFFFFF

def masked(crc):
    return (((crc >> 15) | (crc << 17)) + 0xA282EAD8) & 0xFFFFFFFF

STREAM_ID = b'\xff\x06\x00\x00sNaPpY'

def uncompressed_chunk(payload):
    body = struct.pack('<I', masked(crc32c(payload))) + payload
    sz = len(body)
    header = bytes([0x01, sz & 0xFF, (sz >> 8) & 0xFF, (sz >> 16) & 0xFF])
    return header + body

out = bytearray(STREAM_ID)
empty = uncompressed_chunk(b'')
for _ in range(100000):
    out += empty
out += uncompressed_chunk(b'SELECT 42')

with open(sys.argv[1], 'wb') as f:
    f.write(out)
PY

RESULT=$(${CLICKHOUSE_CURL} -sS --data-binary @"$PAYLOAD_FILE" \
    -H 'Content-Encoding: snappy' \
    "${CLICKHOUSE_URL}")

if [ "$RESULT" = "42" ]; then
    echo "OK: empty snappy chunks handled correctly"
else
    echo "FAIL: expected 42, got '$RESULT'" >&2
fi

rm -f "$PAYLOAD_FILE"
