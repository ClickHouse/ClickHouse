#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


DATA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME.data

formats="Parquet Arrow"

for format in $formats
do
    echo $format
    $CLICKHOUSE_LOCAL -q "select * from generateRandom('a UInt64, a_nullable Nullable(UInt64)', 42) limit 10 format $format" > $DATA_FILE
    $CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE') SETTINGS schema_inference_make_columns_nullable = 'auto'"
    $CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE') SETTINGS schema_inference_make_columns_nullable = 0"
done

for format in $formats
do
    echo $format
    $CLICKHOUSE_LOCAL -q "select * from generateRandom('b Array(UInt64), b_nullable Array(Nullable(UInt64))', 42) limit 10 format $format" > $DATA_FILE
    $CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE') SETTINGS schema_inference_make_columns_nullable = 'auto'"
    $CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE') SETTINGS schema_inference_make_columns_nullable = 0"
done

for format in $formats
do
    echo $format
    $CLICKHOUSE_LOCAL -q "select * from generateRandom('c Tuple(a UInt64, b String), c_nullable Tuple(a Nullable(UInt64), b Nullable(String))', 42) limit 10 format $format" > $DATA_FILE
    $CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE') SETTINGS schema_inference_make_columns_nullable = 'auto'"
    $CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE') SETTINGS schema_inference_make_columns_nullable = 0"
done

for format in $formats
do
    echo $format
    $CLICKHOUSE_LOCAL -q "select * from generateRandom('d Tuple(a UInt64, b Tuple(a UInt64, b String), d_nullable Tuple(a UInt64, b Tuple(a Nullable(UInt64), b Nullable(String))))', 42) limit 10 format $format" > $DATA_FILE
    $CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE') SETTINGS schema_inference_make_columns_nullable = 'auto'"
    $CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE') SETTINGS schema_inference_make_columns_nullable = 0"
done

for format in $formats
do
    echo $format
    $CLICKHOUSE_LOCAL -q "select * from generateRandom('e Map(UInt64, String), e_nullable Map(UInt64, Nullable(String))', 42) limit 10 format $format" > $DATA_FILE
    $CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE') SETTINGS schema_inference_make_columns_nullable = 'auto'"
    $CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE') SETTINGS schema_inference_make_columns_nullable = 0"
done

for format in $formats
do
    echo $format
    $CLICKHOUSE_LOCAL -q "select * from generateRandom('f Map(UInt64, Map(UInt64, String)), f_nullables Map(UInt64, Map(UInt64, Nullable(String)))', 42) limit 10 format $format" > $DATA_FILE
    $CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE') SETTINGS schema_inference_make_columns_nullable = 'auto'"
    $CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE') SETTINGS schema_inference_make_columns_nullable = 0"
done

for format in $formats
do
    echo $format
    $CLICKHOUSE_LOCAL -q "select * from generateRandom('g LowCardinality(String), g_nullable LowCardinality(Nullable(String))', 42) limit 10 settings output_format_arrow_low_cardinality_as_dictionary=1, allow_suspicious_low_cardinality_types=1 format $format" > $DATA_FILE
    $CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE') SETTINGS schema_inference_make_columns_nullable = 'auto'"
    $CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE') SETTINGS schema_inference_make_columns_nullable = 0"
done

rm $DATA_FILE

