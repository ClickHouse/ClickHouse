#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_parquet/02716_data.parquet', auto, 'date Date32')" 2>&1 | grep -c "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE"

$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_parquet/02716_data.parquet', auto, 'date Int32')"

$CLICKHOUSE_LOCAL -q "select date::Int32 from file('$CURDIR/data_parquet/02716_data.parquet', auto, 'date Date32') settings date_time_overflow_behavior='saturate'"
