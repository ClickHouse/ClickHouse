#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, use-hdfs

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "insert into table function file('test_03243.parquet', 'Parquet') select number as i from numbers(100000) settings output_format_parquet_row_group_size=10000,engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select max(i) from file('test_03243.parquet', 'Parquet') settings max_threads = 1;"
$CLICKHOUSE_CLIENT -q "insert into table function hdfs('hdfs://localhost:12222/test_03243.parquet', 'Parquet') select number as i from numbers(100000) settings output_format_parquet_row_group_size=10000,hdfs_truncate_on_insert=1;"
$CLICKHOUSE_CLIENT -q "select max(i) from hdfs('hdfs://localhost:12222/test_03243.parquet', 'Parquet') settings max_threads = 1;"