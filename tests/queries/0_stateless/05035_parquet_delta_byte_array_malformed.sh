#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_parquet"

# The lengths of a DELTA_BYTE_ARRAY page are themselves DELTA_BINARY_PACKED. A page that declares
# zero of them used to make the decoder write the first value through a zero-sized buffer.
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('$DATA/05035_delta_byte_array_zero_values.parquet') FORMAT Null" 2>&1 \
    | grep -o -m1 'INCORRECT_DATA\|CANNOT_PARSE\|Too few values'

# A BYTE_ARRAY column with a Decimal logical type is read into a ColumnDecimal, not a ColumnString.
# The filtered branch of the DELTA_BYTE_ARRAY decoder used to cast the destination unconditionally.
${CLICKHOUSE_LOCAL} --query "
SELECT value FROM file('$DATA/05035_delta_byte_array_decimal.parquet')
PREWHERE keep = 1
ORDER BY value
SETTINGS
    input_format_parquet_max_block_size = 1,
    input_format_parquet_use_offset_index = 0,
    input_format_parquet_filter_push_down = 0,
    input_format_parquet_page_filter_push_down = 0,
    input_format_parquet_bloom_filter_push_down = 0"

${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('$DATA/05035_delta_byte_array_decimal.parquet') ORDER BY value"
