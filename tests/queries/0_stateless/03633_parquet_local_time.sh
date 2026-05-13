#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select toTypeName(ts), * from file('$CUR_DIR/data_parquet/not_utc.parquet', Parquet) settings input_format_parquet_local_time_as_utc=1, session_timezone='Europe/Amsterdam'"
$CLICKHOUSE_LOCAL -q "select toTypeName(ts), * from file('$CUR_DIR/data_parquet/not_utc.parquet', Parquet) settings input_format_parquet_local_time_as_utc=0, session_timezone='Europe/Amsterdam'"
