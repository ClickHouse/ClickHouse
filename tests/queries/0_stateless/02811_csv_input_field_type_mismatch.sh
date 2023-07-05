#!/usr/bin/env bash

# NOTE: this sh wrapper is required because of shell_config

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists test_tbl"
$CLICKHOUSE_CLIENT -q "create table test_tbl (x String, y UInt32, z Date) engine=MergeTree order by x"
cat $CURDIR/data_csv/csv_with_diff_field_types.csv | ${CLICKHOUSE_CLIENT} -q "INSERT INTO test_tbl SETTINGS input_format_csv_allow_check_deserialize=true, input_format_csv_set_default_if_deserialize_failed=true FORMAT CSV"
$CLICKHOUSE_CLIENT -q "select * from test_tbl"
$CLICKHOUSE_CLIENT -q "drop table test_tbl"