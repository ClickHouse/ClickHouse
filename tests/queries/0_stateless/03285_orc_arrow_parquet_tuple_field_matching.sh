#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

# Tests settings input_format_arrow_case_insensitive_column_matching, input_format_orc_case_insensitive_column_matching, and
# input_format_parquet_case_insensitive_column_matching with Tuple values.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

formats="Arrow Parquet ORC";

for format in ${formats}; do
    echo $format

    ${CLICKHOUSE_CLIENT} --query="INSERT INTO function file('tuples.$format', '$format', 'c Tuple(aaa Int64, bbb String, ccc Tuple(ddd Int64, eee String))') select (1, '2', (3, '4')) SETTINGS engine_file_truncate_on_insert = 1"

    ${CLICKHOUSE_CLIENT} --query="SELECT * FROM file('tuples.$format', '$format', 'c Tuple(AAA Int64, BBB String, CCC Tuple(DDD Int64, EEE String))') SETTINGS input_format_arrow_case_insensitive_column_matching = 0, input_format_orc_case_insensitive_column_matching = 0, input_format_parquet_case_insensitive_column_matching = 0"
    ${CLICKHOUSE_CLIENT} --query="SELECT * FROM file('tuples.$format', '$format', 'c Tuple(AAA Int64, BBB String, CCC Tuple(DDD Int64, EEE String))') SETTINGS input_format_arrow_case_insensitive_column_matching = 1, input_format_orc_case_insensitive_column_matching = 1, input_format_parquet_case_insensitive_column_matching = 1"
done
