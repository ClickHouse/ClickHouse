#!/usr/bin/env bash
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/114045: ingesting a gzip
# HTTP body whose DEFLATE payload is a single block spanning the whole stream - the shape
# zlib-ng's deflate_quick path (compression level 1, the default of the official .NET SDK)
# emits. The streaming decompressor used to re-decode the block from its start on every socket
# refill, making the ingest quadratic in the compressed size; a correct decoder handles this
# shape in linear time. No available encoder produces the shape on demand, so the gzip file is
# crafted directly: with the static Huffman code, every literal byte below 144 is an 8-bit
# codeword, so the block body is a byte translation of the payload shifted by the 3 header bits.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

GZFILE=${CLICKHOUSE_TMP}/04836_single_block.gz

python3 -c "
import hashlib, struct, sys, zlib
raw = b''.join(hashlib.sha256(b'%d' % i).hexdigest().encode() + b'\n' for i in range(130000))
assert all(b < 144 for b in raw)
table = bytes(int(format(0x30 + b, '08b')[::-1], 2) for b in range(144)) + bytes(112)
body = raw.translate(table)
# 3 header bits (BFINAL=1, BTYPE=01 static), 8 bits per literal, 7 zero bits of end-of-block.
n = (int.from_bytes(body, 'little') << 3) | 0b011
deflate = n.to_bytes(len(body) + 2, 'little')
blob = (b'\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff' + deflate
        + struct.pack('<II', zlib.crc32(raw), len(raw) % (1 << 32)))
assert zlib.decompress(blob, 31) == raw
open(sys.argv[1], 'wb').write(blob)
" "${GZFILE}"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_04836 (s String) ENGINE = MergeTree ORDER BY ()"

${CLICKHOUSE_CURL} -sS -X POST -H 'Content-Encoding: gzip' -H 'Transfer-Encoding: chunked' \
    -T "${GZFILE}" "${CLICKHOUSE_URL}&query=INSERT%20INTO%20t_04836%20FORMAT%20LineAsString"

${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(cityHash64(s)) FROM t_04836"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_04836"
rm -f "${GZFILE}"
