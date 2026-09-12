#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Valid `IntervalKind::Kind` values are 0x00..0x0A. Any byte above 0x0A is
# rejected by `decodeDataType` so it cannot reach `magic_enum::enum_name` on
# an out-of-range enum (which used to feed a null `std::string_view` into
# `SerializationInterval::getHash` and trip UBSan in `SipHash::update`).

# Bytes per column: `01` num_columns, `01` name_len, `74` ('t'), `22`
# `BinaryTypeIndex::Interval`, then the `IntervalKind` byte.

$CLICKHOUSE_LOCAL -q "select x'010174220b' format TSVRaw" | $CLICKHOUSE_LOCAL --input-format RowBinaryWithNamesAndTypes --input_format_binary_decode_types_in_binary_format -q "select * from table" 2>&1 | grep "Unknown IntervalKind.*INCORRECT_DATA)" -o
$CLICKHOUSE_LOCAL -q "select x'010174227f' format TSVRaw" | $CLICKHOUSE_LOCAL --input-format RowBinaryWithNamesAndTypes --input_format_binary_decode_types_in_binary_format -q "select * from table" 2>&1 | grep "Unknown IntervalKind.*INCORRECT_DATA)" -o
$CLICKHOUSE_LOCAL -q "select x'01017422ff' format TSVRaw" | $CLICKHOUSE_LOCAL --input-format RowBinaryWithNamesAndTypes --input_format_binary_decode_types_in_binary_format -q "select * from table" 2>&1 | grep "Unknown IntervalKind.*INCORRECT_DATA)" -o
