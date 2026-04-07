#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=$CURDIR/test_$CLICKHOUSE_TEST_UNIQUE_NAME.data

echo "Avro"

$CLICKHOUSE_LOCAL -q "select toInt8(-number) as int8, toUInt8(number) as uint8, toInt16(-number) as int16, toUInt16(number) as uint16, toInt32(-number) as int32, toUInt32(number) as uint32, toInt64(-number) as int64, toUInt64(number) as uint64 from numbers(2) format Avro" > $DATA_FILE
$CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE', 'Avro')"
$CLICKHOUSE_LOCAL -q "select * from file('$DATA_FILE', 'Avro')"

$CLICKHOUSE_LOCAL -q "select toFloat32(number * 1.2) as float32, toFloat64(number / 1.3) as float64 from numbers(2) format Avro" > $DATA_FILE
$CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE', 'Avro')"
$CLICKHOUSE_LOCAL -q "select * from file('$DATA_FILE', 'Avro')"

$CLICKHOUSE_LOCAL -q "select toDate(number) as date from numbers(2) format Avro" > $DATA_FILE
$CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE', 'Avro')"
$CLICKHOUSE_LOCAL -q "select * from file('$DATA_FILE', 'Avro')"

$CLICKHOUSE_LOCAL -q "select concat('Str: ', toString(number)) as str, toFixedString(toString((number + 1) * 100 % 1000), 3) as fixed_string from numbers(2) format Avro" > $DATA_FILE
$CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE', 'Avro')"
$CLICKHOUSE_LOCAL -q "select * from file('$DATA_FILE', 'Avro')"
 
$CLICKHOUSE_LOCAL -q "select [number, number + 1] as array, [[[number], [number + 1]]] as nested from numbers(2) format Avro" > $DATA_FILE
$CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE', 'Avro')"
$CLICKHOUSE_LOCAL -q "select * from file('$DATA_FILE', 'Avro')"

rm $DATA_FILE

