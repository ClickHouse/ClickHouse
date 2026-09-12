#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The binary type encoding of `Row` round-trips through `RowBinaryWithNamesAndTypes` and `Native`,
# like the other types in 03173_row_binary_and_native_with_binary_encoded_types.

function test
{
    $CLICKHOUSE_LOCAL --stacktrace --allow_experimental_row_type=1 --output_format_binary_encode_types_in_binary_format=1 -q "select $1 as value format RowBinaryWithNamesAndTypes" | $CLICKHOUSE_LOCAL --input-format RowBinaryWithNamesAndTypes --allow_experimental_row_type=1 --input_format_binary_decode_types_in_binary_format=1 -q "select value, toTypeName(value) from table"
    $CLICKHOUSE_LOCAL --stacktrace --allow_experimental_row_type=1 --output_format_native_encode_types_in_binary_format=1 -q "select $1 as value format Native" | $CLICKHOUSE_LOCAL --input-format Native --allow_experimental_row_type=1 --input_format_native_decode_types_in_binary_format=1 -q "select value, toTypeName(value) from table"
}

test "materialize(tuple(1, 'str', 42.42))::Row(a UInt32, b String, c Float32)"
test "materialize(tuple(1, tuple('str', tuple(42.42, -30))))::Row(a UInt32, b Row(c String, d Tuple(e Float32, f Int8)))"
