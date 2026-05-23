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
