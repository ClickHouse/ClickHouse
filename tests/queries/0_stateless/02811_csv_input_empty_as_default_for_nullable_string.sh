#!/usr/bin/env bash

# NOTE: this sh wrapper is required because of shell_config

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists test_tbl"
# Column `c` is Nullable(String) and `e` is LowCardinality(Nullable(String)); both are exercised with a missing value.
$CLICKHOUSE_CLIENT -q "create table test_tbl (a String, b String, c Nullable(String), e LowCardinality(Nullable(String)), d String) engine=MergeTree order by a"

# By default (input_format_csv_missing_nullable_as_empty_string = 0) an empty unquoted value of a nullable string column
# (both Nullable(String) and LowCardinality(Nullable(String))) is read as NULL when input_format_csv_empty_as_default is enabled.
printf 'a1,b1,,,d1\na2,b2,,,d2\n' | $CLICKHOUSE_CLIENT -q "INSERT INTO test_tbl SETTINGS input_format_csv_empty_as_default = 1, input_format_csv_missing_nullable_as_empty_string = 0 FORMAT CSV"
$CLICKHOUSE_CLIENT -q "select * from test_tbl order by a"
$CLICKHOUSE_CLIENT -q "truncate table test_tbl"

# With input_format_csv_missing_nullable_as_empty_string = 1, the empty value of a nullable string column
# is read as an empty string instead of NULL, regardless of input_format_csv_empty_as_default.
printf 'a1,b1,,,d1\na2,b2,,,d2\n' | $CLICKHOUSE_CLIENT -q "INSERT INTO test_tbl SETTINGS input_format_csv_empty_as_default = 1, input_format_csv_missing_nullable_as_empty_string = 1 FORMAT CSV"
$CLICKHOUSE_CLIENT -q "select * from test_tbl order by a"

$CLICKHOUSE_CLIENT -q "drop table test_tbl"
