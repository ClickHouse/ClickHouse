#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the PNG format requires libpng and base64, and the fast-test build does not enable base64

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The image encoder runs on the server only when the output is produced over HTTP; the native protocol
# formats the result on the client. The Sixel encoder allocates a per-band color buffer proportional to the
# image width (~24 MB at the 1000000 width limit) regardless of the height, so a one-row image isolates that
# scratch buffer from the tiny pixel buffer. The allocation must respect 'max_memory_usage' and be rejected
# under a tight limit, rather than silently exceeding it.
QUERY="SELECT toUInt8(number % 256) AS v FROM numbers(1000000) FORMAT PNG"
SIXEL="output_format_image_width=1000000&output_format_image_height=1&output_format_image_terminal_mode=sixel"

echo "tight limit rejected:"
curl -sS "${CLICKHOUSE_URL}&${SIXEL}&max_memory_usage=10000000" --data-binary "${QUERY}" \
    | grep -oE "MEMORY_LIMIT_EXCEEDED" | head -1

# A generous limit produces a real Sixel stream (starts with the DCS introducer ESC P q == 1b 50 71).
echo "generous limit produces sixel output:"
OUT="${CLICKHOUSE_TMP}/04339_png_format_memory_limit.bin"
curl -sS "${CLICKHOUSE_URL}&${SIXEL}&max_memory_usage=1000000000" --data-binary "${QUERY}" > "${OUT}"
head -c 3 "${OUT}" | od -An -tx1 | tr -d ' \n'
echo
rm -f "${OUT}"

# The iTerm and Kitty modes build the whole encoded PNG in a memory-tracked buffer before transmitting it.
# An incompressible image keeps that buffer about the size of the pixel data, so a tight limit must reject it.
# 'wait_end_of_query=0' streams the response, so the buffered HTTP response does not mask the encoder's usage.
ITERM="output_format_image_width=1000000&output_format_image_height=20&output_format_image_terminal_mode=iterm&wait_end_of_query=0"
echo "iterm tight limit rejected:"
curl -sS "${CLICKHOUSE_URL}&${ITERM}&max_memory_usage=40000000" \
    --data-binary "SELECT toUInt8(sipHash64(number) % 256) AS v FROM numbers(20000000) FORMAT PNG" \
    | grep -oE "MEMORY_LIMIT_EXCEEDED" | head -1
