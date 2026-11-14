#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "Parquet"

$CLICKHOUSE_LOCAL -q "select toFixedString(toString(number % 10), 1) as fixed from numbers(10) format Parquet" --output_format_parquet_fixed_string_as_fixed_byte_array=0  |  $CLICKHOUSE_LOCAL --input-format=Parquet --table test -q "desc test"

$CLICKHOUSE_LOCAL -q "select toFixedString(toString(number % 10), 1) as fixed from numbers(5) format Parquet" --output_format_parquet_fixed_string_as_fixed_byte_array=0  |  $CLICKHOUSE_LOCAL --input-format=Parquet --table test -q "select * from test"

$CLICKHOUSE_LOCAL -q "select toFixedString(toString(number % 10), 1) as fixed from numbers(10) format Parquet" --output_format_parquet_fixed_string_as_fixed_byte_array=1  |  $CLICKHOUSE_LOCAL --input-format=Parquet --table test -q "desc test"

$CLICKHOUSE_LOCAL -q "select toFixedString(toString(number % 10), 1) as fixed from numbers(5) format Parquet" --output_format_parquet_fixed_string_as_fixed_byte_array=1  |  $CLICKHOUSE_LOCAL --input-format=Parquet --table test -q "select * from test"

$CLICKHOUSE_LOCAL -q "select number % 2 ? toFixedString(toString(number % 10), 1) : NULL as fixed from numbers(10) format Parquet" --output_format_parquet_fixed_string_as_fixed_byte_array=1  | $CLICKHOUSE_LOCAL --input-format=Parquet --table test -q "select * from test"

echo "Arrow"

$CLICKHOUSE_LOCAL -q "select toFixedString(toString(number % 10), 1) as fixed from numbers(5) format Arrow" --output_format_arrow_fixed_string_as_fixed_byte_array=0 | $CLICKHOUSE_LOCAL --input-format=Arrow --table test -q "desc test"

$CLICKHOUSE_LOCAL -q "select toFixedString(toString(number % 10), 1) as fixed from numbers(5) format Arrow" --output_format_arrow_fixed_string_as_fixed_byte_array=0 | $CLICKHOUSE_LOCAL --input-format=Arrow --table test -q "select * from test"

$CLICKHOUSE_LOCAL -q "select toFixedString(toString(number % 10), 1) as fixed from numbers(5) format Arrow" --output_format_arrow_fixed_string_as_fixed_byte_array=1 | $CLICKHOUSE_LOCAL --input-format=Arrow --table test -q "desc test"

$CLICKHOUSE_LOCAL -q "select toFixedString(toString(number % 10), 1) as fixed from numbers(5) format Arrow" --output_format_arrow_fixed_string_as_fixed_byte_array=1 | $CLICKHOUSE_LOCAL --input-format=Arrow --table test -q "select * from test"

$CLICKHOUSE_LOCAL -q "select number % 2 ? toFixedString(toString(number % 10), 1) : NULL as fixed from numbers(5) format Arrow" --output_format_arrow_fixed_string_as_fixed_byte_array=1 | $CLICKHOUSE_LOCAL --input-format=Arrow --table test -q "select * from test"

