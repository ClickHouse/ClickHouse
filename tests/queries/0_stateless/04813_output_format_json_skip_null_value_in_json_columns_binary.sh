#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL \
    --enable_json_type=1 \
    --output_format_native_write_json_as_string=1 \
    --output_format_json_skip_null_value_in_json_columns=1 \
    -q "SELECT '{\"a\":null,\"b\":1}'::JSON(a Nullable(UInt32), b UInt32) FORMAT Native" \
    | grep -aoF '{"b":1}'

$CLICKHOUSE_LOCAL \
    --enable_json_type=1 \
    --output_format_binary_write_json_as_string=1 \
    --output_format_json_skip_null_value_in_json_columns=1 \
    -q "SELECT '{\"a\":null,\"b\":1}'::JSON(a Nullable(UInt32), b UInt32) FORMAT RowBinary" \
    | grep -aoF '{"b":1}'
