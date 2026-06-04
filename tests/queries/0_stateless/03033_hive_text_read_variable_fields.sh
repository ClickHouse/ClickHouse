#!/usr/bin/env bash
# Tags: no-fasttest
# NOTE: this sh wrapper is required because of shell_config

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists test_tbl"
$CLICKHOUSE_CLIENT -q "create table test_tbl (a UInt16, b UInt32, c UInt32) engine=MergeTree order by a"
$CLICKHOUSE_CLIENT -q "insert into test_tbl from infile '$CURDIR/data_hive/fields_number_variable.txt' SETTINGS input_format_hive_text_fields_delimiter=',' FORMAT HIVETEXT"
$CLICKHOUSE_CLIENT -q "select * from test_tbl"
$CLICKHOUSE_CLIENT -q "drop table test_tbl"