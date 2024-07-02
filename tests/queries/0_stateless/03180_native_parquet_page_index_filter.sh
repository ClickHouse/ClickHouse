#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

PAR_PATH="$CURDIR"/data_parquet/native_parquet_page_index_reader.parquet

# native_parquet_page_index_reader.parquet contains 30000 rows, s is 128 bytes strings.
CH_SCHEMA="\
    id Nullable(Int64), \
    s Nullable(String)"

QUERY="SELECT count(s) from file('$PAR_PATH', 'Parquet', '$CH_SCHEMA') where id > 25000"
$CLICKHOUSE_LOCAL --multiquery --max_threads 1 --max_parsing_threads 1 --input_format_parquet_use_native_reader true --query "$QUERY"

