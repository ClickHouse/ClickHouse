#!/usr/bin/env bash

# NOTE: this sh wrapper is required because of shell_config

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists test_whitespace"
$CLICKHOUSE_CLIENT -q "drop table if exists test_tab"
$CLICKHOUSE_CLIENT -q "create table test_whitespace (x UInt32, y String, z String) engine=MergeTree order by x"
$CLICKHOUSE_CLIENT -q "create table test_tab (x UInt32, y String, z String) engine=MergeTree order by x"
cat $CURDIR/data_csv/csv_with_space_delimiter.csv | ${CLICKHOUSE_CLIENT} -q "INSERT INTO test_whitespace SETTINGS format_csv_delimiter=' ', input_format_csv_allow_whitespace_or_tab_as_delimiter=true FORMAT CSV"
cat $CURDIR/data_csv/csv_with_tab_delimiter.csv | ${CLICKHOUSE_CLIENT} -q "INSERT INTO test_tab SETTINGS format_csv_delimiter='\t', input_format_csv_allow_whitespace_or_tab_as_delimiter=true FORMAT CSV"
$CLICKHOUSE_CLIENT -q "select * from test_whitespace"
$CLICKHOUSE_CLIENT -q "select * from test_tab"
$CLICKHOUSE_CLIENT -q "drop table test_whitespace"
$CLICKHOUSE_CLIENT -q "drop table test_tab"; 
