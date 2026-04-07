#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for format in Native Parquet ORC Arrow Avro BSONEachRow MsgPack JSONEachRow CSV TSV Values
do
    echo $format
    $CLICKHOUSE_LOCAL -q "select number % 2 ? NULL : number as n, [number, number % 2 ? NULL : number] as arr1, [[[number, number % 2 ? NULL : number], [NULL]]] as arr2, tuple('hello', number % 2 ? NULL : number, number) as tup1, tuple('Hello', tuple('Hello', tuple(number, number % 2 ? NULL : number)), number) as tup2, map(number, number % 2 ? NULL : number) as map1, map(number, map(number, number % 2 ? null : number)) as map2, tuple('Hello', [tuple('Hello', map(number, number % 2 ? NULL : number), [number % 2 ? NULL : number])], number) as nested from numbers(5) format $format" > $CLICKHOUSE_TEST_UNIQUE_NAME.$format
    $CLICKHOUSE_LOCAL -q "select * from file('$CLICKHOUSE_TEST_UNIQUE_NAME.$format', auto, 'n UInt64 default 42, arr1 Array(UInt64), arr2 Array(Array(Array(UInt64))), tup1 Tuple(String, UInt64, UInt64), tup2 Tuple(String, Tuple(String, Tuple(UInt64, UInt64)), UInt64), map1 Map(UInt64, UInt64), map2 Map(UInt64, Map(UInt64, UInt64)), nested Tuple(String, Array(Tuple(String, Map(UInt64, UInt64), Array(UInt64))), UInt64)') settings input_format_null_as_default=1"
    rm $CLICKHOUSE_TEST_UNIQUE_NAME.$format
done

