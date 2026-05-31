#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires libpng (not built in fast test)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

OUT="${CLICKHOUSE_TMP}/04259_png_format"
mkdir -p "${OUT}"

check_png_header()
{
    local file="$1"
    local expected_w="$2"
    local expected_h="$3"
    local expected_color_type="$4"

    # PNG signature: 89 50 4E 47 0D 0A 1A 0A
    local sig
    sig=$(head -c 8 "$file" | od -An -tx1 | tr -d ' \n')
    if [[ "$sig" != "89504e470d0a1a0a" ]]; then
        echo "BAD SIGNATURE: $sig"
        return
    fi

    # IHDR chunk follows. width at bytes 16..19, height at bytes 20..23,
    # color_type at byte 25.
    local w h color
    w=$(printf '%d' "0x$(dd if="$file" bs=1 skip=16 count=4 2>/dev/null | od -An -tx1 | tr -d ' \n')")
    h=$(printf '%d' "0x$(dd if="$file" bs=1 skip=20 count=4 2>/dev/null | od -An -tx1 | tr -d ' \n')")
    color=$(printf '%d' "0x$(dd if="$file" bs=1 skip=25 count=1 2>/dev/null | od -An -tx1 | tr -d ' \n')")
    echo "width=$w height=$h color_type=$color"
    if [[ "$w" != "$expected_w" || "$h" != "$expected_h" || "$color" != "$expected_color_type" ]]; then
        echo "MISMATCH: expected ${expected_w}x${expected_h} color_type=${expected_color_type}"
    fi
}

# RGB implicit (color_type=2)
$CLICKHOUSE_CLIENT --output_format_image_width=4 --output_format_image_height=2 --query "
    SELECT toUInt8(number * 60) AS r, toUInt8(number * 30) AS g, toUInt8(255 - number * 50) AS b
    FROM numbers(8) FORMAT PNG
" > "${OUT}/rgb.png"
check_png_header "${OUT}/rgb.png" 4 2 2

# RGBA implicit (color_type=6)
$CLICKHOUSE_CLIENT --output_format_image_width=2 --output_format_image_height=2 --query "
    SELECT toUInt8(number * 60) AS r, toUInt8(number * 30) AS g, toUInt8(255 - number * 50) AS b, toUInt8(number * 80) AS a
    FROM numbers(4) FORMAT PNG
" > "${OUT}/rgba.png"
check_png_header "${OUT}/rgba.png" 2 2 6

# Grayscale (integer 'v'), color_type=0
$CLICKHOUSE_CLIENT --output_format_image_width=3 --output_format_image_height=3 --query "
    SELECT toUInt8(number * 30) AS v FROM numbers(9) FORMAT PNG
" > "${OUT}/gray.png"
check_png_header "${OUT}/gray.png" 3 3 0

# Grayscale (float 'v' in [0, 1] mapped to [0, 255]), color_type=0
$CLICKHOUSE_CLIENT --output_format_image_width=2 --output_format_image_height=2 --query "
    SELECT toFloat64(number) / 3 AS v FROM numbers(4) FORMAT PNG
" > "${OUT}/gray_float.png"
check_png_header "${OUT}/gray_float.png" 2 2 0

# Binary 'v' (Bool), color_type=0 (rendered as 8-bit grayscale)
$CLICKHOUSE_CLIENT --output_format_image_width=2 --output_format_image_height=2 --query "
    SELECT (number % 2 = 0) AS v FROM numbers(4) FORMAT PNG
" > "${OUT}/binary.png"
check_png_header "${OUT}/binary.png" 2 2 0

# Explicit coordinates. Out-of-range coords are silently ignored.
$CLICKHOUSE_CLIENT --output_format_image_width=4 --output_format_image_height=4 --query "
    WITH data AS (
        SELECT * FROM VALUES('x Int32, y Int32, r UInt8, g UInt8, b UInt8',
            (0, 0, 255, 0, 0),
            (-1, 0, 0, 255, 0),
            (4, 0, 0, 0, 255),
            (1, 1, 100, 100, 100),
            (1, 1, 200, 200, 200)
        )
    )
    SELECT x, y, r, g, b FROM data FORMAT PNG
" > "${OUT}/explicit.png"
check_png_header "${OUT}/explicit.png" 4 4 2

# Defaults: 1024x1024, RGB color_type=2
$CLICKHOUSE_CLIENT --query "
    SELECT toUInt8(0) AS r, toUInt8(0) AS g, toUInt8(0) AS b FROM numbers(1) FORMAT PNG
" > "${OUT}/default.png"
check_png_header "${OUT}/default.png" 1024 1024 2

# --- Pixel value validation ---
# Decode the PNG (8-bit, no interlace) and print requested pixels as "x,y=c0,c1,...".
# This guards the pixel mapping itself: a regression that zeroes pixels, ignores explicit
# coordinates, or breaks clamping/float scaling would still pass the IHDR-only checks above.
DECODER="${OUT}/decode_png.py"
cat > "${DECODER}" <<'PYEOF'
import sys, zlib, struct

def decode(path):
    data = open(path, "rb").read()
    assert data[:8] == b"\x89PNG\r\n\x1a\n", "bad signature"
    pos, width, height, color_type, idat = 8, 0, 0, 0, b""
    while pos < len(data):
        length = struct.unpack(">I", data[pos:pos + 4])[0]
        ctype = data[pos + 4:pos + 8]
        chunk = data[pos + 8:pos + 8 + length]
        if ctype == b"IHDR":
            width, height, _bit_depth, color_type = struct.unpack(">IIBB", chunk[:10])
        elif ctype == b"IDAT":
            idat += chunk
        elif ctype == b"IEND":
            break
        pos += 12 + length
    raw = zlib.decompress(idat)
    channels = {0: 1, 2: 3, 6: 4}[color_type]
    stride, prev, out, p = width * channels, bytearray(width * channels), bytearray(), 0
    for _ in range(height):
        f = raw[p]; p += 1
        line = bytearray(raw[p:p + stride]); p += stride
        for i in range(stride):
            a = line[i - channels] if i >= channels else 0
            b = prev[i]
            c = prev[i - channels] if i >= channels else 0
            if f == 1: line[i] = (line[i] + a) & 0xff
            elif f == 2: line[i] = (line[i] + b) & 0xff
            elif f == 3: line[i] = (line[i] + ((a + b) >> 1)) & 0xff
            elif f == 4:
                pp = a + b - c
                pa, pb, pc = abs(pp - a), abs(pp - b), abs(pp - c)
                line[i] = (line[i] + (a if pa <= pb and pa <= pc else (b if pb <= pc else c))) & 0xff
        out += line
        prev = line
    return width, channels, out

w, ch, px = decode(sys.argv[1])
for arg in sys.argv[2:]:
    x, y = map(int, arg.split(","))
    off = (y * w + x) * ch
    print(arg + "=" + ",".join(str(v) for v in px[off:off + ch]))
PYEOF

# Implicit RGB, filled in scanline (row-major) order.
$CLICKHOUSE_CLIENT --output_format_image_width=2 --output_format_image_height=1 --query "
    SELECT * FROM VALUES('r UInt8, g UInt8, b UInt8', (10, 20, 30), (40, 50, 60)) FORMAT PNG
" > "${OUT}/px_rgb.png"
python3 "${DECODER}" "${OUT}/px_rgb.png" 0,0 1,0

# Explicit coordinates: the later write to the same pixel wins; untouched pixels stay black.
$CLICKHOUSE_CLIENT --output_format_image_width=2 --output_format_image_height=1 --query "
    SELECT * FROM VALUES('x Int32, y Int32, r UInt8, g UInt8, b UInt8', (0, 0, 1, 2, 3), (0, 0, 9, 8, 7)) FORMAT PNG
" > "${OUT}/px_explicit.png"
python3 "${DECODER}" "${OUT}/px_explicit.png" 0,0 1,0

# Float grayscale clamping: <0 -> 0, in [0,1] -> scaled to [0,255], >1 -> 255.
$CLICKHOUSE_CLIENT --output_format_image_width=3 --output_format_image_height=1 --query "
    SELECT * FROM VALUES('v Float64', (-0.5), (0.5), (2.0)) FORMAT PNG
" > "${OUT}/px_float.png"
python3 "${DECODER}" "${OUT}/px_float.png" 0,0 1,0 2,0

# Binary (Bool) grayscale: true -> 255, false -> 0.
$CLICKHOUSE_CLIENT --output_format_image_width=2 --output_format_image_height=1 --query "
    SELECT * FROM VALUES('v Bool', (true), (false)) FORMAT PNG
" > "${OUT}/px_binary.png"
python3 "${DECODER}" "${OUT}/px_binary.png" 0,0 1,0

# PNG is a complete datastream, so appending another image to an existing PNG file is rejected.
$CLICKHOUSE_LOCAL --query "INSERT INTO FUNCTION file('${OUT}/append.png', 'PNG') SELECT toUInt8(0) AS v FROM numbers(1)" 2>&1 1>/dev/null
$CLICKHOUSE_LOCAL --query "INSERT INTO FUNCTION file('${OUT}/append.png', 'PNG') SELECT toUInt8(0) AS v FROM numbers(1)" 2>&1 1>/dev/null | grep -oE "CANNOT_APPEND_TO_FILE" | head -1

# Dimension above the PNG 31-bit limit -> exception (not silently truncated)
${CLICKHOUSE_CLIENT} --output_format_image_width=4294967297 --query "
    SELECT toUInt8(0) AS v FROM numbers(1) FORMAT PNG
" 2>&1 1>/dev/null | grep -oE "BAD_ARGUMENTS" | head -1

# Unknown column -> exception
${CLICKHOUSE_CLIENT} --query "
    SELECT number AS foo FROM numbers(1) FORMAT PNG
" 2>&1 1>/dev/null | grep -oE "BAD_ARGUMENTS" | head -1

# Ambiguous: mixing v with r,g,b -> exception
${CLICKHOUSE_CLIENT} --query "
    SELECT toUInt8(1) AS r, toUInt8(2) AS g, toUInt8(3) AS b, toUInt8(4) AS v FROM numbers(1) FORMAT PNG
" 2>&1 1>/dev/null | grep -oE "BAD_ARGUMENTS" | head -1

# Only one of x/y -> exception
${CLICKHOUSE_CLIENT} --query "
    SELECT toInt32(1) AS x, toUInt8(0) AS r, toUInt8(0) AS g, toUInt8(0) AS b FROM numbers(1) FORMAT PNG
" 2>&1 1>/dev/null | grep -oE "BAD_ARGUMENTS" | head -1

# Missing one of r,g,b -> exception
${CLICKHOUSE_CLIENT} --query "
    SELECT toUInt8(1) AS r, toUInt8(2) AS g FROM numbers(1) FORMAT PNG
" 2>&1 1>/dev/null | grep -oE "BAD_ARGUMENTS" | head -1

rm -rf "${OUT}"
