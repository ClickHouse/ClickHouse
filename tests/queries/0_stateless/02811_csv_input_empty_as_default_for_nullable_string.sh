#!/usr/bin/env bash

# NOTE: this sh wrapper is required because of shell_config

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists test_tbl"
$CLICKHOUSE_CLIENT -q "create table test_tbl (a String, b String, c Nullable(String), d Nullable(String)) engine=MergeTree order by a"
cat $CURDIR/data_csv/csv_empty_for_string_field.csv | ${CLICKHOUSE_CLIENT} -q "INSERT INTO test_tbl SETTINGS input_format_csv_empty_as_default=true, input_format_csv_empty_string_is_not_null=true FORMAT CSV"
$CLICKHOUSE_CLIENT -q "select * from test_tbl"
$CLICKHOUSE_CLIENT -q "drop table test_tbl"