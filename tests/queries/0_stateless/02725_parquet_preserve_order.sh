#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


# This file has a row group with 2 rows, then a row group with 1 row.
# It'll be read into two blocks. The first block will sleep 2x longer than the second.
# So reordering is very likely if the order-preservation doesn't work.

$CLICKHOUSE_LOCAL -q "select number + sleepEachRow(3) from file('$CURDIR/data_parquet/02725_data.parquet') settings input_format_parquet_preserve_order=1, function_sleep_max_microseconds_per_block = 6000000, input_format_parquet_local_file_min_bytes_for_seek=0"

$CLICKHOUSE_LOCAL -q "explain pipeline select number + sleepEachRow(3) from file('$CURDIR/data_parquet/02725_data.parquet') settings input_format_parquet_preserve_order=1, max_threads=2, input_format_parquet_local_file_min_bytes_for_seek=0"
$CLICKHOUSE_LOCAL -q "explain pipeline select number + sleepEachRow(3) from file('$CURDIR/data_parquet/02725_data.parquet') settings input_format_parquet_preserve_order=0, parallelize_output_from_storages=1, max_threads=2, input_format_parquet_local_file_min_bytes_for_seek=0"
