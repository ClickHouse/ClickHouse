#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
    set input_format_parquet_use_native_reader_v3=1;
    select sum(*) from file('$CUR_DIR/data_parquet/int32_decimal.parquet') prewhere max2(0, value::Int64 - 10);
    select sum(*) from file('$CUR_DIR/data_parquet/int32_decimal.parquet') prewhere toNullable(max2(0, value::Int64 - 10));
    select sum(*) from file('$CUR_DIR/data_parquet/int32_decimal.parquet') prewhere toLowCardinality(max2(0, value::Int64 - 10));
    select sum(*) from file('$CUR_DIR/data_parquet/int32_decimal.parquet') prewhere toLowCardinality(toNullable(max2(0, value::Int64 - 10)));"