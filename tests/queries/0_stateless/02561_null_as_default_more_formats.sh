#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists test"
$CLICKHOUSE_CLIENT -q "create table test (x UInt64 default 42, y UInt64, z LowCardinality(String) default '42') engine=Memory";
for format in Parquet Arrow ORC Avro MsgPack Native
do
    echo $format
    $CLICKHOUSE_CLIENT -q "select number % 2 ? NULL : number as x, x as y, CAST(number % 2 ? NULL : toString(number), 'LowCardinality(Nullable(String))') as z from numbers(2) format $format" | $CLICKHOUSE_CLIENT -q "insert into test settings input_format_null_as_default=0 format $format" 2>&1 | grep "Exception" -c
    $CLICKHOUSE_CLIENT -q "select number % 2 ? NULL : number as x, x as y, CAST(number % 2 ? NULL : toString(number), 'LowCardinality(Nullable(String))') as z from numbers(2) format $format settings output_format_arrow_low_cardinality_as_dictionary=1" | $CLICKHOUSE_CLIENT -q "insert into test settings input_format_null_as_default=1, input_format_defaults_for_omitted_fields=0 format $format"
    $CLICKHOUSE_CLIENT -q "select * from test"
    $CLICKHOUSE_CLIENT -q "truncate table test"
    $CLICKHOUSE_CLIENT -q "select number % 2 ? NULL : number as x, x as y, CAST(number % 2 ? NULL : toString(number), 'LowCardinality(Nullable(String))') as z from numbers(2) format $format settings output_format_arrow_low_cardinality_as_dictionary=1" | $CLICKHOUSE_CLIENT -q "insert into test settings input_format_null_as_default=1, input_format_defaults_for_omitted_fields=1 format $format"
    $CLICKHOUSE_CLIENT -q "select * from test"
    $CLICKHOUSE_CLIENT -q "truncate table test"
done

