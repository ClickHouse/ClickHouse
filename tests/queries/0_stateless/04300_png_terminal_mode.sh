#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires libpng (not built in fast test)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

OUT="${CLICKHOUSE_TMP}/04300_png_terminal_mode"
mkdir -p "${OUT}"

QUERY="SELECT toUInt8(number * 60) AS r, toUInt8(number * 30) AS g, toUInt8(255 - number * 50) AS b FROM numbers(8) FORMAT PNG"

# iTerm2: the base64 payload between ':' and BEL must decode to a valid PNG.
$CLICKHOUSE_CLIENT --output_format_image_width=4 --output_format_image_height=2 --output_format_image_terminal_mode='iterm' --query "$QUERY" > "${OUT}/iterm"
python3 - "${OUT}/iterm" <<'EOF'
import base64, sys
data = open(sys.argv[1], 'rb').read()
ok = data.startswith(b'\x1b]1337;File=inline=1;size=')
payload = data[data.index(b':') + 1 : data.index(b'\x07')]
png = base64.b64decode(payload)
print('iterm:', 'OK' if ok and png[:8] == b'\x89PNG\r\n\x1a\n' else 'BAD')
EOF

# Kitty: concatenated base64 chunks must decode to a valid PNG.
$CLICKHOUSE_CLIENT --output_format_image_width=4 --output_format_image_height=2 --output_format_image_terminal_mode='kitty' --query "$QUERY" > "${OUT}/kitty"
python3 - "${OUT}/kitty" <<'EOF'
import base64, re, sys
data = open(sys.argv[1], 'rb').read()
ok = data.startswith(b'\x1b_Gf=100,a=T,')
chunks = re.findall(rb'\x1b_G[^;]*;([^\x1b]*)\x1b\\', data)
png = base64.b64decode(b''.join(chunks))
print('kitty:', 'OK' if ok and png[:8] == b'\x89PNG\r\n\x1a\n' else 'BAD')
EOF

# Sixel: starts with the DCS introducer and raster attributes, ends with the string terminator.
$CLICKHOUSE_CLIENT --output_format_image_width=4 --output_format_image_height=2 --output_format_image_terminal_mode='sixel' --query "$QUERY" > "${OUT}/sixel"
python3 - "${OUT}/sixel" <<'EOF'
import sys
data = open(sys.argv[1], 'rb').read()
ok = data.startswith(b'\x1bPq') and b'"1;1;4;2' in data and data.rstrip(b'\n').endswith(b'\x1b\\')
print('sixel:', 'OK' if ok else 'BAD')
EOF

# 'auto' with non-terminal output falls back to raw PNG bytes.
$CLICKHOUSE_CLIENT --output_format_image_width=4 --output_format_image_height=2 --output_format_image_terminal_mode='auto' --query "$QUERY" > "${OUT}/auto"
sig=$(head -c 8 "${OUT}/auto" | od -An -tx1 | tr -d ' \n')
[[ "$sig" == "89504e470d0a1a0a" ]] && echo "auto: OK" || echo "auto: BAD"

# Invalid mode -> exception.
$CLICKHOUSE_CLIENT --output_format_image_terminal_mode='nonsense' --query "$QUERY" 2>&1 1>/dev/null | grep -oE "BAD_ARGUMENTS" | head -1

rm -rf "${OUT}"
