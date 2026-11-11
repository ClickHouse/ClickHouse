#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the PNG format requires base64, and the fast-test build does not enable base64

# Regression test for the raw `FORMAT PNG` output path: the compressed pixel data must be emitted as a
# stream of bounded `IDAT` chunks, not materialized as one in-memory chunk. A large, incompressible image
# therefore produces several `IDAT` chunks, and peak memory stays bounded instead of growing with the image.
# See https://github.com/ClickHouse/ClickHouse/pull/110051.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

OUT="${CLICKHOUSE_TMP}/04511_png_format_streaming_idat"
mkdir -p "${OUT}"

# An incompressible one-row image, wide enough that its compressed data spans more than one bounded
# `IDAT` chunk. Render over HTTP so the image is produced by the server-side encoder.
N=200000
VALUE="toUInt8(sipHash64(number) % 256)"
curl -sS "${CLICKHOUSE_URL}&output_format_image_width=${N}&output_format_image_height=1" \
    --data-binary "SELECT ${VALUE} AS v FROM numbers(${N}) FORMAT PNG" > "${OUT}/big.png"

# The exact pixel bytes in scanline order, used to confirm the decoded image round-trips byte for byte
# (fetched from the server so the check does not depend on reproducing the hash outside ClickHouse). A byte
# sum would let corruptions that preserve the total (reordered, duplicated, dropped, or substituted bytes)
# slip through, so compare the whole decoded pixel buffer against this exact reference instead.
${CLICKHOUSE_CLIENT} --query "SELECT ${VALUE} AS v FROM numbers(${N}) FORMAT RowBinary" > "${OUT}/pixels.bin"

DECODER="${OUT}/decode.py"
cat > "${DECODER}" <<'PYEOF'
import sys, zlib, struct

path, truth_path = sys.argv[1], sys.argv[2]
data = open(path, "rb").read()
truth = open(truth_path, "rb").read()

print("signature ok" if data[:8] == b"\x89PNG\r\n\x1a\n" else "bad signature")

pos, idat, idat_chunks, width, height, color_type, crc_ok = 8, b"", 0, 0, 0, 0, True
while pos < len(data):
    length = struct.unpack(">I", data[pos:pos + 4])[0]
    ctype = data[pos + 4:pos + 8]
    chunk = data[pos + 8:pos + 8 + length]
    crc_stored = struct.unpack(">I", data[pos + 8 + length:pos + 12 + length])[0]
    if (zlib.crc32(ctype + chunk) & 0xffffffff) != crc_stored:
        crc_ok = False
    if ctype == b"IHDR":
        width, height, _bit_depth, color_type = struct.unpack(">IIBB", chunk[:10])
    elif ctype == b"IDAT":
        idat += chunk
        idat_chunks += 1
    elif ctype == b"IEND":
        break
    pos += 12 + length

print("all chunk crcs ok" if crc_ok else "crc mismatch")
print("multiple IDAT chunks: yes" if idat_chunks > 1 else "multiple IDAT chunks: no")

raw = zlib.decompress(idat)
channels = {0: 1, 2: 3, 6: 4}[color_type]
stride = width * channels

# Each scanline is prefixed with a filter-type byte; the encoder picks the best of the five standard PNG
# filters per row, so validate the type and undo the filter to reconstruct the raw pixels.
def paeth(a, b, c):
    p = a + b - c
    pa, pb, pc = abs(p - a), abs(p - b), abs(p - c)
    return a if pa <= pb and pa <= pc else (b if pb <= pc else c)

filters_ok, pixels, prev, pos = True, bytearray(), bytearray(stride), 0
for _ in range(height):
    ftype = raw[pos]; pos += 1
    if ftype > 4:
        filters_ok = False
    line = bytearray(raw[pos:pos + stride]); pos += stride
    for i in range(stride):
        a = line[i - channels] if i >= channels else 0
        b = prev[i]
        c = prev[i - channels] if i >= channels else 0
        if ftype == 1: line[i] = (line[i] + a) & 0xff
        elif ftype == 2: line[i] = (line[i] + b) & 0xff
        elif ftype == 3: line[i] = (line[i] + ((a + b) >> 1)) & 0xff
        elif ftype == 4: line[i] = (line[i] + paeth(a, b, c)) & 0xff
    pixels += line
    prev = line
print("filters valid ok" if filters_ok else "invalid filter")
print("pixels round-trip ok" if bytes(pixels) == truth else "pixels mismatch")
PYEOF

python3 "${DECODER}" "${OUT}/big.png" "${OUT}/pixels.bin"

# The chunk-count check above proves the final file is split into several `IDAT` chunks, but on its own it
# would still pass an implementation that first buffered the whole compressed image in memory and only sliced
# it into chunks at serialization time - exactly the regression fixed in this PR. Pin the actual invariant
# (constant peak memory) with a memory limit that the streaming encoder fits but a full-buffering one cannot.
#
# The pixel buffer P is inherent and present either way; here P = 1000000 x 64 x 1 byte = 64 MiB (the image is
# incompressible, so the compressed copy C is ~= P = 64 MiB). The streaming encoder keeps only P plus a small
# bounded `IDAT` chunk, so its peak is ~= P + a few MiB; the old encoder held P and the full compressed copy at
# once, so its peak was >= P + C = 128 MiB. A 100 MB limit leaves comfortable headroom above the streaming peak
# yet is far below P + C, so this encode succeeds now and would fail with `MEMORY_LIMIT_EXCEEDED` on the old code.
# `max_memory_usage` bounds the tracked ClickHouse allocations (the pixel buffer and, on the old path, the
# compressed buffer), so the check is independent of RSS, the build's sanitizer overhead, and parallel load.
# Run it through `clickhouse-local` so the encoder (not just query execution) is covered by the limit - over the
# native protocol `FORMAT PNG` is applied client-side and would not be constrained by the server-side limit.
MEM_W=1000000
MEM_H=64
MEM_N=$((MEM_W * MEM_H))
# `max_threads` and `max_block_size` are pinned so the transient input blocks stay small regardless of the
# randomized session settings the test harness may inject (a large `max_block_size` alone would make
# `numbers` materialize a huge block and exceed the limit for reasons unrelated to the encoder).
if ${CLICKHOUSE_LOCAL} --query "
        SELECT toUInt8(sipHash64(number)) AS v FROM numbers(${MEM_N}) FORMAT PNG
        SETTINGS output_format_image_width = ${MEM_W}, output_format_image_height = ${MEM_H},
                 max_threads = 1, max_block_size = 65505, max_memory_usage = 100000000" > /dev/null 2>&1
then
    echo "streaming under memory limit: ok"
else
    echo "streaming under memory limit: FAILED"
fi

rm -rf "${OUT}"
