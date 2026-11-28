#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=$CURDIR/test_$CLICKHOUSE_TEST_UNIQUE_NAME.data

for format in TSVWithNamesAndTypes TSVRawWithNamesAndTypes CSVWithNamesAndTypes JSONCompactEachRowWithNamesAndTypes JSONCompactStringsEachRowWithNamesAndTypes RowBinaryWithNamesAndTypes CustomSeparatedWithNamesAndTypes
do
    echo $format
    $CLICKHOUSE_LOCAL -q "select toInt8(-number) as int8, toUInt8(number) as uint8, toInt16(-number) as int16, toUInt16(number) as uint16, toInt32(-number) as int32, toUInt32(number) as uint32, toInt64(-number) as int64, toUInt64(number) as uint64 from numbers(2) format $format" > $DATA_FILE
    $CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE', '$format')"
    $CLICKHOUSE_LOCAL -q "select * from file('$DATA_FILE', '$format')"

    $CLICKHOUSE_LOCAL -q "select toFloat32(number * 1.2) as float32, toFloat64(number / 1.3) as float64, toDecimal32(number / 0.3, 5) as decimal32, toDecimal64(number / 0.003, 5) as decimal64 from numbers(2) format $format" > $DATA_FILE
    $CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE', '$format')"
    $CLICKHOUSE_LOCAL -q "select * from file('$DATA_FILE', '$format')"

    $CLICKHOUSE_LOCAL -q "select toDate(number) as date, toDate32(number) as date32 from numbers(2) format $format" > $DATA_FILE
    $CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE', '$format')"
    $CLICKHOUSE_LOCAL -q "select * from file('$DATA_FILE', '$format')"

    $CLICKHOUSE_LOCAL -q "select concat('Str: ', toString(number)) as str, toFixedString(toString((number + 1) * 100 % 1000), 3) as fixed_string from numbers(2) format $format" > $DATA_FILE
    $CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE', '$format')"
    $CLICKHOUSE_LOCAL -q "select * from file('$DATA_FILE', '$format')"
 
    $CLICKHOUSE_LOCAL -q "select [number, number + 1] as array, (number, toString(number)) as tuple, map(toString(number), number) as map from numbers(2) format $format" > $DATA_FILE
    $CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE', '$format')"
    $CLICKHOUSE_LOCAL -q "select * from file('$DATA_FILE', '$format')"

    $CLICKHOUSE_LOCAL -q "select [([number, number + 1], map('42', number)), ([], map()), ([42], map('42', 42))] as nested1, (([[number], [number + 1], []], map(number, [(number, '42'), (number + 1, '42')])), 42) as nested2 from numbers(2) format $format" > $DATA_FILE
    $CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE', '$format')"
    $CLICKHOUSE_LOCAL -q "select * from file('$DATA_FILE', '$format')"
done

rm $DATA_FILE

