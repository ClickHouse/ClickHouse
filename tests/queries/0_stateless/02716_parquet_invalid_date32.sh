#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The day number 200000 (year 2517) is within the Date32 range [0000-01-01, 9999-12-31].
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_parquet/02716_data.parquet', auto, 'date Date32')"

$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_parquet/02716_data.parquet', auto, 'date Int32')"

$CLICKHOUSE_LOCAL -q "select date::Int32 from file('$CURDIR/data_parquet/02716_data.parquet', auto, 'date Date32') settings date_time_overflow_behavior='saturate'"

# A day number beyond 9999-12-31 is out of range: an error by default, the boundary value with 'saturate'.
# Date32 arithmetic does not clamp, so an out-of-range day number can be written to the file.
INVALID_PARQUET=${CLICKHOUSE_TMP}/02716_invalid_date32.parquet
$CLICKHOUSE_LOCAL -q "insert into function file('$INVALID_PARQUET', Parquet, 'date Date32') select toDate32('9999-12-31') + 100 settings engine_file_truncate_on_insert = 1"

$CLICKHOUSE_LOCAL -q "select * from file('$INVALID_PARQUET', auto, 'date Date32')" 2>&1 | grep -c "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE"

$CLICKHOUSE_LOCAL -q "select date::Int32 from file('$INVALID_PARQUET', auto, 'date Date32') settings date_time_overflow_behavior='saturate'"

rm -f "$INVALID_PARQUET"
