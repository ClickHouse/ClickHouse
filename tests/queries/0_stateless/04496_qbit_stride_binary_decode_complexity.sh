#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A strided QBit expands a tiny binary type header into `element_size * (dimension / stride)` hidden FixedString streams.
# Such a header must be charged against `input_format_binary_max_type_complexity`, otherwise a few bytes decode into an
# unreasonably wide type. The payloads below are RowBinaryWithNamesAndTypes headers (one column named `x`) whose type is a
# binary-encoded QBit with an explicit stride (tag 0x37).

check() {
    $CLICKHOUSE_CLIENT --query "$1" 2>&1 | grep -q 'Binary type decoding complexity limit exceeded' && echo "rejected" || echo "accepted"
}

# QBit(Float64, 512, 8) is a valid type: 512 / 8 = 64 stride groups, well under MAX_STRIDE_GROUPS = 1024. But it materializes
# element_size * num_strides = 64 * 64 = 4096 FixedString streams, so it must be rejected under the default complexity budget (1000).
check "SELECT * FROM format(RowBinaryWithNamesAndTypes, x'010178370e800408') SETTINGS input_format_binary_decode_types_in_binary_format=1, input_format_binary_max_type_complexity=1000"

# The same header decodes once the budget is raised above the hidden stream count, proving the rejection comes from the
# complexity budget rather than the MAX_STRIDE_GROUPS cap.
check "SELECT * FROM format(RowBinaryWithNamesAndTypes, x'010178370e800408') SETTINGS input_format_binary_decode_types_in_binary_format=1, input_format_binary_max_type_complexity=100000"

# A small strided QBit stays well within the default budget: QBit(Float32, 16, 8) has 32 * (16 / 8) = 64 streams.
check "SELECT * FROM format(RowBinaryWithNamesAndTypes, x'010178370d1008') SETTINGS input_format_binary_decode_types_in_binary_format=1"
