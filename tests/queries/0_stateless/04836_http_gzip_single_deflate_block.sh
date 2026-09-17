#!/usr/bin/env bash
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/114045: ingesting a gzip
# HTTP body whose DEFLATE payload is a single block spanning the whole stream - the shape
# zlib-ng's deflate_quick path (compression level 1, the default of the official .NET SDK)
# emits. The streaming decompressor used to re-decode the block from its start on every socket
# refill, making the ingest quadratic in the compressed size; a correct decoder handles this
# shape in linear time. No available encoder produces the shape on demand, so the gzip file is
# crafted directly: with the static Huffman code, every literal byte below 144 is an 8-bit
# codeword, so the block body is a byte translation of the payload shifted by the 3 header bits.
#
# The assertion is on time, because the quadratic decoder still produces the right bytes, just
# far too slowly. An absolute budget is not robust: the linear ingest of a 36 MB body takes
# 0.2 s on a release build but well over 15 s under TSan when the flaky check runs 18 copies of
# the test at once. So the same payload is ingested twice - first as an ordinary multi-block
# gzip, which every decoder handles in linear time, then as the single block - and the test
# asserts that the single-block ingest is at most 10x slower than the multi-block one. Both
# sides scale together with machine speed and load, so the ratio does not depend on either:
# measured on a release build, the quadratic decoder needs about 22 s for the single block
# against about 0.3 s for the multi-block stream (over 70x), and the linear decoder about the
# same time for both. The quadratic growth was confirmed to hold over the whole range
# (10/20/40/60 MB took 1.8/6.9/27.3/61.1 s), so the pre-fix margin does not depend on the
# machine being as fast as the one measured. The payload is 90 lines of 400 KB rather than many
# short ones so that line parsing and the `MergeTree` write cost nothing next to the
# decompression under test (with 520000 short lines they dominated at 5.8 s). The multi-block
# baseline is ingested first so that any one-off warm-up cost lands on the baseline side.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

MULTI_BLOCK_FILE=${CLICKHOUSE_TMP}/04836_multi_block.gz
SINGLE_BLOCK_FILE=${CLICKHOUSE_TMP}/04836_single_block.gz

python3 -c "
import hashlib, struct, sys, zlib
size, line = 36000000, 400000
chunk = b''.join(hashlib.sha256(b'%d' % i).hexdigest().encode() for i in range(size // 64 + 1))[:size]
raw = b'\n'.join(chunk[i:i + line] for i in range(0, size, line)) + b'\n'

# The baseline: an ordinary gzip stream of many DEFLATE blocks, as any regular encoder emits.
# Level 6 (not 1) so that the shape does not depend on whether Python links zlib or zlib-ng.
multi = zlib.compressobj(6, zlib.DEFLATED, 31)
open(sys.argv[1], 'wb').write(multi.compress(raw) + multi.flush())

# The shape under test: the whole payload as one static-Huffman DEFLATE block.
assert max(raw) < 144
table = bytes(int(format(0x30 + b, '08b')[::-1], 2) for b in range(144)) + bytes(112)
body = raw.translate(table)
# 3 header bits (BFINAL=1, BTYPE=01 static), 8 bits per literal, 7 zero bits of end-of-block.
n = (int.from_bytes(body, 'little') << 3) | 0b011
deflate = n.to_bytes(len(body) + 2, 'little')
blob = (b'\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff' + deflate
        + struct.pack('<II', zlib.crc32(raw), len(raw) % (1 << 32)))
assert zlib.decompress(blob, 31) == raw
open(sys.argv[2], 'wb').write(blob)
" "${MULTI_BLOCK_FILE}" "${SINGLE_BLOCK_FILE}"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_04836 (s String) ENGINE = MergeTree ORDER BY ()"

function ingest()
{
    local start
    start=$(date +%s%N)
    ${CLICKHOUSE_CURL} -sS -X POST -H 'Content-Encoding: gzip' -H 'Transfer-Encoding: chunked' \
        -T "$1" "${CLICKHOUSE_URL}&query=INSERT%20INTO%20t_04836%20FORMAT%20LineAsString"
    echo $(( $(date +%s%N) - start ))
}

MULTI_BLOCK_NS=$(ingest "${MULTI_BLOCK_FILE}")
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(length(s)), sum(cityHash64(s)) FROM t_04836"
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE t_04836"

SINGLE_BLOCK_NS=$(ingest "${SINGLE_BLOCK_FILE}")
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(length(s)), sum(cityHash64(s)) FROM t_04836"

if (( SINGLE_BLOCK_NS > MULTI_BLOCK_NS * 10 )); then
    echo "single-block ingest took ${SINGLE_BLOCK_NS} ns, multi-block ingest took ${MULTI_BLOCK_NS} ns"
fi
echo "single-block ingest within 10x of multi-block: $(( SINGLE_BLOCK_NS <= MULTI_BLOCK_NS * 10 ))"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_04836"
rm -f "${MULTI_BLOCK_FILE}" "${SINGLE_BLOCK_FILE}"
