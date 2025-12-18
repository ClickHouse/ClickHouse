#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test that data type binary decoding rejects overly complex nested types.
# It should be rejected quickly with complexity limit error rather than timing out.

{
    printf '\x01'           # 1 column
    printf '\x01x'          # column name "x"
    printf '\x2A'           # Variant type code
    printf '\xF0\x93\x09'   # varint 150000
    head -c 150000 /dev/zero  # 150000 x 0x00 (Nothing type codes)
} | ${CLICKHOUSE_LOCAL} --input-format=RowBinaryWithNamesAndTypes \
    --input_format_binary_decode_types_in_binary_format=1 \
    -q "SELECT * FROM table" 2>&1 | grep -oF 'Binary type decoding complexity limit exceeded: 1001 > 1000 (adjust input_format_binary_max_type_complexity)'
