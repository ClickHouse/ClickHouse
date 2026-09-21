#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the ceiling belongs to the libdeflate decoder, and fast test builds with
# ENABLE_LIBRARIES=0, so gzip goes through zlib, which never buffers a whole block

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

bomb="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_bomb.gz"
plain="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_plain.gz"
insert="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_insert.gz"

# One fixed-Huffman DEFLATE block that inflates to just over the single-block ceiling: a literal zero
# followed by 260112 copies of (length 258, distance 1). libdeflate suspends only at block boundaries,
# so the whole block has to be buffered before any of it is exposed.
python3 -c '
import sys, zlib, struct
pairs = 260112
out = bytearray(); acc = 0; nb = 0
def emit(val, cnt):
    global acc, nb
    acc |= val << nb; nb += cnt
    while nb >= 8:
        out.append(acc & 0xFF); acc >>= 8; nb -= 8
emit(0b11, 3)       # BFINAL=1, BTYPE=01
emit(0x0C, 8)       # literal 0x00
for _ in range(pairs):
    emit(0xA3, 13)  # length 258, distance 1
emit(0x00, 7)       # end of block
if nb:
    out.append(acc & 0xFF)
size = 1 + 258 * pairs
crc = zlib.crc32(b"\x00" * size) & 0xFFFFFFFF
sys.stdout.buffer.write(b"\x1f\x8b\x08\x00" + b"\x00" * 4 + b"\x00\xff" + bytes(out) + struct.pack("<II", crc, size))
' > "$bomb"

${CLICKHOUSE_CURL} -sS -H 'Content-Encoding: gzip' --data-binary "@$bomb" "${CLICKHOUSE_URL}" \
    | grep -o -m1 'A single gzip block decompresses to more than [0-9]* bytes'

# The ceiling belongs to the request body, not to the gzip codec: the same stream read as a file has
# no ceiling, because that allocation is already covered by the memory limit of the query.
${CLICKHOUSE_LOCAL} -q "SELECT length(raw_blob) FROM file('$bomb', 'RawBLOB')"

# An ordinary gzip body is unaffected.
printf 'SELECT 1' | gzip -c > "$plain"
${CLICKHOUSE_CURL} -sS -H 'Content-Encoding: gzip' --data-binary "@$plain" "${CLICKHOUSE_URL}"

# A stream of many blocks, far larger than one output buffer, still streams.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t (x UInt64) ENGINE = Memory"
python3 -c '
import gzip, sys
rows = b"".join(b"%d\n" % i for i in range(300000))
sys.stdout.buffer.write(gzip.compress(b"INSERT INTO t FORMAT CSV\n" + rows))
' > "$insert"
${CLICKHOUSE_CURL} -sS -H 'Content-Encoding: gzip' --data-binary "@$insert" "${CLICKHOUSE_URL}"
${CLICKHOUSE_CLIENT} -q "SELECT count(), max(x) FROM t"

rm -f "$bomb" "$plain" "$insert"
