#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA="$CURDIR/data_parquet"

# Every fixture declares a DECIMAL precision that maps to a narrower ClickHouse type than one
# encoded value occupies (INT64 or FIXED_LEN_BYTE_ARRAY with precision 9 -> Decimal32). Reading
# any of them used to write past the end of the destination column.

echo '-- dictionary-encoded INT64, declared precision 9'
$CLICKHOUSE_LOCAL -q "SELECT count(), sum(k) FROM file('$DATA/04670_decimal_int64_precision9_dict.parquet', Parquet)"

echo '-- plain-encoded INT64, declared precision 9'
$CLICKHOUSE_LOCAL -q "SELECT count(), sum(k) FROM file('$DATA/04670_decimal_int64_precision9_plain.parquet', Parquet)"

echo '-- FIXED_LEN_BYTE_ARRAY type_length 8, declared precision 9'
$CLICKHOUSE_LOCAL -q "SELECT count(), sum(k) FROM file('$DATA/04670_decimal_flba8_precision9.parquet', Parquet)"

echo '-- FIXED_LEN_BYTE_ARRAY type_length 16, declared precision 9 (skips two width buckets)'
$CLICKHOUSE_LOCAL -q "SELECT count(), sum(k) FROM file('$DATA/04670_decimal_flba16_precision9.parquet', Parquet)"

echo '-- filter push-down over the narrowed column'
$CLICKHOUSE_LOCAL -q "SELECT count(), sum(k) FROM file('$DATA/04670_decimal_int64_precision9_dict.parquet', Parquet) WHERE k > 50000 SETTINGS input_format_parquet_filter_push_down = 1"

echo '-- schema inference still reports the declared precision'
$CLICKHOUSE_LOCAL -q "DESC file('$DATA/04670_decimal_int64_precision9_dict.parquet', Parquet)"

echo '-- explicit wider type hint reads without narrowing'
$CLICKHOUSE_LOCAL -q "SELECT toTypeName(k), count(), sum(k) FROM file('$DATA/04670_decimal_int64_precision9_dict.parquet', Parquet, 'k Decimal(18, 2)') GROUP BY 1"

echo '-- a value that exceeds the declared precision is an error, not a corrupted read'
$CLICKHOUSE_LOCAL -q "SELECT count(), sum(k) FROM file('$DATA/04670_decimal_int64_precision9_overflow.parquet', Parquet)" 2>&1 | grep -o -m1 'DECIMAL_OVERFLOW'

echo '-- ... and reads losslessly with a wide enough hint'
$CLICKHOUSE_LOCAL -q "SELECT count(), sum(k) FROM file('$DATA/04670_decimal_int64_precision9_overflow.parquet', Parquet, 'k Decimal(18, 2)')"
