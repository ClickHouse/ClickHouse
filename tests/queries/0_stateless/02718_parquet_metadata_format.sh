#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_parquet/02718_data.parquet', ParquetMetadata) format JSONEachRow" | python3 -m json.tool

$CLICKHOUSE_LOCAL -q "select num_columns, format_version from file('$CURDIR/data_parquet/02718_data.parquet', ParquetMetadata, 'num_columns UInt64, format_version String') format JSONEachRow" | python3 -m json.tool

$CLICKHOUSE_LOCAL -q "select columns from file('$CURDIR/data_parquet/02718_data.parquet', ParquetMetadata) format JSONEachRow" | python3 -m json.tool


$CLICKHOUSE_LOCAL -q "select some_column from file('$CURDIR/data_parquet/02718_data.parquet', ParquetMetadata, 'some_column Array(UInt32)')" 2>&1 | grep -c "BAD_ARGUMENTS"

$CLICKHOUSE_LOCAL -q "select num_columns from file('$CURDIR/data_parquet/02718_data.parquet', ParquetMetadata, 'num_columns Array(UInt32)')" 2>&1 | grep -c "BAD_ARGUMENTS"


$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_parquet/ipv6_bloom_filter.gz.parquet', ParquetMetadata) format JSONEachRow" | python3 -m json.tool
