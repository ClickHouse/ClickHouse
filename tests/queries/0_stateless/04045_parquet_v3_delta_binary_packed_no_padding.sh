#!/usr/bin/env bash
# Tags: no-fasttest

# Test that the Parquet v3 reader correctly handles DELTA_BINARY_PACKED encoding
# where the last miniblock is not zero-padded to the full miniblock size.
# Some writers (e.g. parquet-go) don't pad, causing "Unexpected end of page data"
# in strict decoders. See: https://github.com/ClickHouse/ClickHouse/issues/93093

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

cp "$CURDIR/data_parquet/04045_delta_no_padding_3vals.parquet" "$USER_FILES_PATH/"
cp "$CURDIR/data_parquet/04045_delta_no_padding_5vals.parquet" "$USER_FILES_PATH/"
cp "$CURDIR/data_parquet/04045_delta_sample_93093.parquet" "$USER_FILES_PATH/"

# Unpadded files: only the v3 reader handles these (tests the fix).
echo "--- 3 values, no padding, v3 ---"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('04045_delta_no_padding_3vals.parquet') ORDER BY x SETTINGS input_format_parquet_use_native_reader_v3=1"

echo "--- 5 values, no padding, v3 ---"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('04045_delta_no_padding_5vals.parquet') ORDER BY x SETTINGS input_format_parquet_use_native_reader_v3=1"

# Real-world file from issue #93093 (pyarrow-generated, properly padded).
# Both readers must produce the same result.
echo "--- #93093 sample, v3 ---"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('04045_delta_sample_93093.parquet') SETTINGS input_format_parquet_use_native_reader_v3=1"

echo "--- #93093 sample, v1 ---"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('04045_delta_sample_93093.parquet') SETTINGS input_format_parquet_use_native_reader_v3=0"
