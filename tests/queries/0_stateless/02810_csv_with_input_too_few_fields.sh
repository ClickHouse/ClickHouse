#!/usr/bin/env bash

# NOTE: this sh wrapper is required because of shell_config

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists test_tbl"
$CLICKHOUSE_CLIENT -q "create table test_tbl (x UInt32, y String, z String) engine=MergeTree order by x"
cat $CURDIR/data_csv/csv_with_diff_number_fields.csv | ${CLICKHOUSE_CLIENT} -q "INSERT INTO test_tbl SETTINGS format_csv_delimiter=' ', input_format_csv_allow_set_column_default_value_if_no_input=true FORMAT CSV"
$CLICKHOUSE_CLIENT -q "select * from test_tbl"
$CLICKHOUSE_CLIENT -q "drop table test_tbl"; 
