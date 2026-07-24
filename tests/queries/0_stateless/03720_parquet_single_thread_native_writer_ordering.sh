#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


$CLICKHOUSE_LOCAL -q "select * from numbers(10) settings max_block_size=1, output_format_parquet_use_custom_encoder=1, max_threads=1 format Parquet" | $CLICKHOUSE_LOCAL -q "select * from table" --input-format=Parquet
