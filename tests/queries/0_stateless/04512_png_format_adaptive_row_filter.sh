#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the PNG format requires base64, and the fast-test build does not enable base64

# Regression test for adaptive per-row filtering in the native `FORMAT PNG` encoder. The removed `libpng`
# path chose the best of the five standard PNG scanline filters per row; a naive encoder that always emits the
# "None" filter bloats the output on filter-friendly images (gradients, photos). This test renders such an
# image and checks that the encoder still picks non-trivial filters and that the encoded pixel data is far
# smaller than the always-"None" path would produce for the same pixels.
# See https://github.com/ClickHouse/ClickHouse/pull/110051.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

OUT="${CLICKHOUSE_TMP}/04512_png_format_adaptive_row_filter"
mkdir -p "${OUT}"

W=256
H=256
N=$((W * H))

# The pixel at (x, y) (row-major order) is (sipHash64(x) + y) mod 256. The vertical direction is a constant
# +1 step, so the "Up" filter turns every row after the first into a constant residual that Deflate crushes;
# horizontally the sipHash scramble has no run structure, so the raw ("None") bytes barely compress. This is
# exactly the kind of filter-friendly image on which always emitting "None" would blow up the output size.
VALUE="toUInt8((sipHash64(number % ${W}) + intDiv(number, ${W})) % 256)"

# Render over HTTP so the image is produced by the server-side encoder.
curl -sS "${CLICKHOUSE_URL}&output_format_image_width=${W}&output_format_image_height=${H}" \
    --data-binary "SELECT ${VALUE} AS v FROM numbers(${N}) FORMAT PNG" > "${OUT}/gradient.png"

# The exact pixel bytes in scanline order, used to confirm the decoded image round-trips byte for byte
# (fetched from the server so the check does not depend on reproducing the hash outside ClickHouse).
${CLICKHOUSE_CLIENT} --query "SELECT ${VALUE} AS v FROM numbers(${N}) FORMAT RowBinary" > "${OUT}/pixels.bin"

DECODER="${OUT}/decode.py"
cat > "${DECODER}" <<'PYEOF'
import sys, zlib, struct

path, truth_path, width, height = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
data = open(path, "rb").read()
truth = open(truth_path, "rb").read()
print("signature ok" if data[:8] == b"\x89PNG\r\n\x1a\n" else "bad signature")

pos, color_type, idat = 8, 0, b""
while pos < len(data):
    length = struct.unpack(">I", data[pos:pos + 4])[0]
    ctype = data[pos + 4:pos + 8]
    chunk = data[pos + 8:pos + 8 + length]
    if ctype == b"IHDR":
        _w, _h, _bit_depth, color_type = struct.unpack(">IIBB", chunk[:10])
    elif ctype == b"IDAT":
        idat += chunk
    elif ctype == b"IEND":
        break
    pos += 12 + length

channels = {0: 1, 2: 3, 6: 4}[color_type]
stride = width * channels
raw = zlib.decompress(idat)

def paeth(a, b, c):
    p = a + b - c
    pa, pb, pc = abs(p - a), abs(p - b), abs(p - c)
    return a if pa <= pb and pa <= pc else (b if pb <= pc else c)

# Undo the per-row filters to reconstruct the pixels, tracking whether any non-"None" filter was chosen.
used_non_none, pixels, prev, p = False, bytearray(), bytearray(stride), 0
for _ in range(height):
    ftype = raw[p]; p += 1
    if ftype != 0:
        used_non_none = True
    line = bytearray(raw[p:p + stride]); p += stride
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

print("pixels round-trip ok" if bytes(pixels) == truth else "pixels mismatch")
print("non-None filter used: yes" if used_non_none else "non-None filter used: no")

# Self-calibrating size check, independent of platform and zlib version: compress the same pixels with the
# always-"None" filter (a leading 0 byte per row) at the same Deflate level, and compare with the actual
# adaptively-filtered stream. Adaptive filtering makes this image more than 10x smaller than "None".
none_stream = b"".join(b"\x00" + bytes(pixels[y * stride:(y + 1) * stride]) for y in range(height))
none_size = len(zlib.compress(none_stream, 6))
actual_size = len(idat)
print("adaptive much smaller than None: yes" if actual_size * 10 < none_size
      else "adaptive much smaller than None: no (actual=%d none=%d)" % (actual_size, none_size))
PYEOF

python3 "${DECODER}" "${OUT}/gradient.png" "${OUT}/pixels.bin" "${W}" "${H}"

rm -rf "${OUT}"
