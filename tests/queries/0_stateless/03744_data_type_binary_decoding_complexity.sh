#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test that data type binary decoding rejects overly complex nested types.
# It should be rejected quickly with complexity limit error rather than timing out.

python3 -c '
import sys

def write_varint(n):
    while n >= 0x80:
        sys.stdout.buffer.write(bytes([n & 0x7F | 0x80]))
        n >>= 7
    sys.stdout.buffer.write(bytes([n]))

# Total nodes: 1 + 10000 * 1000000
outer_elements = 10000
inner_elements = 999999

# 1 column
write_varint(1)

# Column name length and name
write_varint(2)
sys.stdout.buffer.write(b"c0")

# Column type: Tuple(Tuple(UInt8, ...), ...)
# 0x1F = UnnamedTuple, 0x01 = UInt8
sys.stdout.buffer.write(b"\x1F")
write_varint(outer_elements)
for _ in range(outer_elements):
    sys.stdout.buffer.write(b"\x1F")
    write_varint(inner_elements)
    sys.stdout.buffer.write(b"\x01" * inner_elements)
' 2>/dev/null | ${CLICKHOUSE_LOCAL} --input-format=RowBinaryWithNamesAndTypes \
    --input_format_binary_decode_types_in_binary_format=1 \
    -q "SELECT * FROM table" 2>&1 | grep -o 'Binary type decoding complexity limit exceeded'