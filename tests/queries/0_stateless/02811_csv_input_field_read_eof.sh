#!/usr/bin/env bash

# NOTE: this sh wrapper is required because of shell_config

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists test_tbl"
$CLICKHOUSE_CLIENT -q "create table test_tbl (a String, b String, c Int32, d Int32) engine=MergeTree order by a"
cat $CURDIR/data_csv/csv_with_field_read_eof.csv | ${CLICKHOUSE_CLIENT} -q "INSERT INTO test_tbl SETTINGS input_format_csv_use_default_on_bad_values=true, input_format_csv_allow_variable_number_of_columns=true  FORMAT CSV"
$CLICKHOUSE_CLIENT -q "select * from test_tbl"
$CLICKHOUSE_CLIENT -q "drop table test_tbl"