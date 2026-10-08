#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Control: Nullable(Array(UInt8)) has complexity 3, should be rejected with limit 2.
$CLICKHOUSE_CLIENT --query "SELECT * FROM format(RowBinaryWithNamesAndTypes, x'010178231e01') SETTINGS input_format_binary_decode_types_in_binary_format=1, input_format_binary_max_type_complexity=2" 2>&1 | grep -o -m1 'Binary type decoding complexity limit exceeded'

# Nested AggregateFunction(sum, ...) also exceeds complexity 2, should be rejected.
$CLICKHOUSE_CLIENT --query "SELECT * FROM format(RowBinaryWithNamesAndTypes, x'010178' || repeat(x'25000373756d0001', 3) || x'01') SETTINGS input_format_binary_decode_types_in_binary_format=1, input_format_binary_max_type_complexity=2" 2>&1 | grep -o -m1 'Binary type decoding complexity limit exceeded'
