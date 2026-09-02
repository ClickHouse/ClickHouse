#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME-data
SCHEMA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME-schema

function test_structure()
{
    format=$1
    ext=$2
    structure=$3

    $CLICKHOUSE_LOCAL -q "select structureTo${format}Schema('$structure') format TSVRaw" > $SCHEMA_FILE.$ext
    tail -n +2 $SCHEMA_FILE.$ext

    $CLICKHOUSE_LOCAL -q "select * from generateRandom('$structure', 42) limit 10 format $format settings format_schema='$SCHEMA_FILE:Message', format_capn_proto_enum_comparising_mode='by_names'" > $DATA_FILE
    $CLICKHOUSE_LOCAL -q "select * from file('$DATA_FILE', $format, '$structure') format Null settings format_schema='$SCHEMA_FILE:Message', format_capn_proto_enum_comparising_mode='by_names'"

}

function test_format()
{
    format=$1
    ext=$2

    echo $format

    echo Numbers
    numbers='int8 Int8, uint8 UInt8, int16 Int16, uint16 UInt16, int32 Int32, uint32 UInt32, int64 Int64, uint64 UInt64, int128 Int128, uint128 UInt128, int256 Int256, uint256 UInt256, float32 Float32, float64 Float64, decimal32 Decimal32(3), decimal64 Decimal64(10), decimal128 Decimal128(20), decimal256 Decimal256(40)'
    test_structure $format $ext "$numbers"

    echo Dates
    dates='data Date, date32 Date32, datetime DateTime, datatime64 DateTime64(9)'
    test_structure $format $ext "$dates"

    echo Strings
    strings='string String, fixedstring FixedString(42)'
    test_structure $format $ext "$strings"

    echo Special
    special='ipv4 IPv4, ipv6 IPv6, uuid UUID'
    test_structure $format $ext "$special"

    echo Nullable
    nullable='nullable Nullable(UInt32)'
    test_structure $format $ext "$nullable"

    echo Enums
    enums="enum8 Enum8(''v1'' = -100, ''v2'' = -10, ''v3'' = 0, ''v4'' = 42), enum16 Enum16(''v5'' = -2000, ''v6'' = -1000, ''v7'' = 0, ''v8'' = 1000, ''v9'' = 2000)"
    test_structure $format $ext "$enums"

    echo Arrays
    arrays='arr1 Array(UInt32), arr2 Array(Array(Array(UInt32)))'
    test_structure $format $ext "$arrays"
    
    echo Tuples
    tuples='tuple1 Tuple(e1 UInt32, e2 String, e3 DateTime), tuple2 Tuple(e1 Tuple(e1 UInt32, e2 Tuple(e1 String, e2 DateTime), e3 String), e2 Tuple(e1 String, e2 UInt32))'
    test_structure $format $ext "$tuples"

    echo Maps
    maps='map1 Map(String, UInt32), map2 Map(String, Map(String, Map(String, UInt32)))'
    test_structure $format $ext "$maps"

    echo Complex
    complex='c1 Array(Tuple(e1 Map(String, Array(Array(Nullable(UInt32)))), e2 Map(String, Tuple(e1 Array(Array(Nullable(String))), e2 Nested(e1 UInt32, e2 Tuple(e1 Array(Array(Nullable(String))), e2 UInt32))))))'
    test_structure $format $ext "$complex"

    echo "Read/write with no schema"
    $CLICKHOUSE_LOCAL -q "select * from numbers(10) format $format" > $DATA_FILE
    $CLICKHOUSE_LOCAL -q "select * from file('$DATA_FILE', $format, 'number UInt64')"

    echo "Output schema"
    $CLICKHOUSE_LOCAL -q "select * from numbers(10) format $format settings output_format_schema='$SCHEMA_FILE.$ext'" > $DATA_FILE
    tail -n +2 $SCHEMA_FILE.$ext

    echo "Bad output schema path"
    $CLICKHOUSE_CLIENT -q "insert into function file('$DATA_FILE', $format) select * from numbers(10) settings output_format_schema='/tmp/schema.$ext'" 2>&1 | grep "BAD_ARGUMENTS" -c
    $CLICKHOUSE_CLIENT -q "insert into function file('$DATA_FILE', $format) select * from numbers(10) settings output_format_schema='../../schema.$ext'" 2>&1 | grep "BAD_ARGUMENTS" -c
}

test_format CapnProto capnp
test_format Protobuf proto

rm $DATA_FILE
rm $SCHEMA_FILE*

