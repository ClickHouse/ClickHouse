#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# reset --log_comment
CLICKHOUSE_LOG_COMMENT=
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_variant_type=1 --use_variant_as_common_type=1 --allow_experimental_object_type=1"


function create_table()
{
    $CH_CLIENT -q "create table test (id UInt64, v Variant(String, $2)) engine=$1;"
}

function drop_table()
{
    $CH_CLIENT -q "drop table if exists test;"
}

function run_select()
{
    $CH_CLIENT -nmq "select v.\`$1\` from test;"
}

function test_integer_types()
{
    echo "Integer Types"

    create_table "$1" "UInt8"
    $CH_CLIENT -q  "insert into test select number, (number % 2 ? 'str_' || toString(number) : number::UInt8) from numbers(3);"
    run_select "UInt8"
    drop_table

    create_table "$1" "UInt16"
    $CH_CLIENT -q  "insert into test select number, (number % 2 ? 'str_' || toString(number) : number::UInt16) from numbers(3);"
    run_select "UInt16"
    drop_table

    create_table "$1" "UInt32"
    $CH_CLIENT -q  "insert into test select number, (number % 2 ? 'str_' || toString(number) : number::UInt32) from numbers(3);"
    run_select "UInt32"
    drop_table

    create_table "$1" "UInt64"
    $CH_CLIENT -q  "insert into test select number, (number % 2 ? 'str_' || toString(number) : number) from numbers(3);"
    run_select "UInt64"
    drop_table
    
    create_table "$1" "UInt128"
    $CH_CLIENT -q  "insert into test select number, (number % 2 ? 'str_' || toString(number) : number::UInt128) from numbers(3);"
    run_select "UInt128"
    drop_table

    create_table "$1" "UInt256"
    $CH_CLIENT -q  "insert into test select number, (number % 2 ? 'str_' || toString(number) : number::UInt256) from numbers(3);"
    run_select "UInt256"
    drop_table

    create_table "$1" "Int8"
    $CH_CLIENT -q  "insert into test select number, (number % 2 ? 'str_' || toString(number) : number::Int8) from numbers(3);"
    run_select "Int8"
    drop_table
    
    create_table "$1" "Int16"
    $CH_CLIENT -q  "insert into test select number, (number % 2 ? 'str_' || toString(number) : number::Int16) from numbers(3);"
    run_select "Int16"
    drop_table
    
    create_table "$1" "Int32"
    $CH_CLIENT -q  "insert into test select number, (number % 2 ? 'str_' || toString(number) : number::Int32) from numbers(3);"
    run_select "Int32"
    drop_table
    
    create_table "$1" "Int64"
    $CH_CLIENT -q  "insert into test select number, (number % 2 ? 'str_' || toString(number) : number::Int64) from numbers(3);"
    run_select "Int64"
    drop_table
    
    create_table "$1" "Int128"
    $CH_CLIENT -q  "insert into test select number, (number % 2 ? 'str_' || toString(number) : number::Int128) from numbers(3);"
    run_select "Int128"
    drop_table
    
    create_table "$1" "Int256"
    $CH_CLIENT -q  "insert into test select number, (number % 2 ? 'str_' || toString(number) : number::Int256) from numbers(3);"
    run_select "Int256"
    drop_table
}

function test_floating_point_numbers()
{
    echo "Floating Point Types"

    create_table "$1" "Float32"
    $CH_CLIENT -q  "insert into test select 1, round(1 - 0.9, 4)::Float32 FROM system.numbers LIMIT 3;"
    run_select "Float32"
    drop_table

    create_table "$1" "Float64"
    $CH_CLIENT -q  "insert into test select 1, (1 - 0.9)::Float64 FROM system.numbers LIMIT 3;"
    run_select "Float64"
    drop_table
    
    #create_table "$1" "Decimal32"
    #$CH_CLIENT -q  "insert into test select 1, toDecimal32(round(randCanonical(), 3)::Decimal32 , 3) FROM system.numbers LIMIT 3;"
    #run_select "Decimal32"
    #drop_table
}

function test_boolean()
{
    echo "Boolean Types"

    create_table "$1" "Bool"   # create table works for Boolean but select only works for .Bool
    $CH_CLIENT -q  "insert into test values (1, true);"
    run_select "Bool"
    drop_table
}

function test_strings()
{
    echo "String Types"

    create_table "$1" "String"
    $CH_CLIENT -q  "insert into test values (1, 'Hello');"
    run_select "String"
    drop_table

    create_table "$1" "FixedString(5)"
    #$CH_CLIENT -q  "insert into test values (1, '12345');"
    #run_select "FixedString(5)"  # Cannot select referring to data type FixedString or FixedString(5)
    drop_table
}

function test_dates()
{
    echo "Date Types"

    create_table "$1" "Date"
    $CH_CLIENT -q  "insert into test values (1, '2019-01-01');"
    run_select "Date"
    drop_table

    create_table "$1" "Date32"
    $CH_CLIENT -q  "insert into test values (1, '1969-01-01');"
    run_select "Date32"
    drop_table

    create_table "$1" "DateTime"
    $CH_CLIENT -q  "insert into test values (1, '2019-01-01 00:00:00');"
    run_select "DateTime"
    drop_table

    create_table "$1" "DateTime64"
    $CH_CLIENT -q  "insert into test values (1, '2299-01-01 00:00:00');"
    # run_select "DateTime64" # Cannot select referring to data type DateTime64 or DateTime64(3)
    drop_table
}

function test_json()
{
    echo "JSON Types"

    #create_table "$1" "JSON" # Variant accepts the JSON type with exception : "Code: 36. DB::Exception: Positional options are not supported. (BAD_ARGUMENTS)"
    #CH_CLIENT -q  "insert into test values (1, '{"a": 1, "b": { "c": 2, "d": [1, 2, 3] }}')";
    #run_select "JSON" # select fails with the expected error "unexpectedly has dynamic columns. (LOGICAL_ERROR)"
    #drop_table
}

function test_uuid()
{
    echo "UUID Types"

    create_table "$1" "UUID"
    $CH_CLIENT -q  "insert into test select 1, toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0');"
    run_select "UUID"
    drop_table
}

function test_low_cardinality_types()
{
    echo "Low Cardinality Types"

    create_table "$1" "LowCardinality(String)"
    $CH_CLIENT -q  "insert into test select number, 'str_' || toString(number)::LowCardinality(String) from numbers(10);"
    run_select "LowCardinality(String)"
    drop_table

    #create_table "$1" "Enum"
    #$CH_CLIENT -q  "insert into test run_select number, number % 2, from numbers(5);"
    #run_select "Enum"
    #drop_table
}

function test_arrays()
{
    echo "Array Types"

    create_table "$1" "Array(UInt64)"
    $CH_CLIENT -q  "insert into test select 1, range(number) from numbers(5);"
    run_select "Array(UInt64)"
    drop_table

    create_table "$1" "Array(Variant(UInt64, String))"
    $CH_CLIENT -q  "insert into test select 1, (number % 2) ? 'str_' || toString(number) : array(number, 'str__' || toString(number)) from numbers(5);"
    #run_select "Array(Variant(UInt64, String))" # select doesn't work. Probably because of multiple parentheses in type name
    drop_table
}

function test_maps()
{
    echo "Map Types"

    create_table "$1" "Map(String, String)"
    $CH_CLIENT -q  "insert into test select 1, (number % 2 ? toString(number) : map('a', toString(number), 'b', 'str_' || toString(number))) from numbers(5);"
    run_select "Map(String, String)"
    drop_table
    
    create_table "$1" "Map(String, Variant(UInt8, String))"
    $CH_CLIENT -q  "insert into test select 1, (map('a', toString(number), 'b', number % 2))::Map(String, Variant(UInt8, String)) from numbers(5);"
    #run_select "Map(String, Variant(UInt8, String))"  # select doesn't work. Probably because of multiple parentheses in type name
    drop_table
}

function test_nested_types()
{
    echo "Nested Types"

    create_table "$1" "Nested(i UInt64, s String)" # Create table allowed with Nested type as valid type
    #$CH_CLIENT -q  "insert into test run_select 1, (number % 2 ? toString(number) : map('a', toString(number), 'b', 'str_' || toString(number) ) from numbers(5);"
    #run_select "Nested(i UInt64, s String)"
    drop_table
}

function test_tuples()
{
    echo "Tuple Types"

    create_table "$1" "Tuple(i UInt8, s String)"
    $CH_CLIENT -q  "insert into test select number, tuple(number, 'str_' || toString(number))::Tuple(i UInt8, s String) from numbers(5);"
    run_select "Tuple(i UInt8, s String)"
    drop_table
}

function test_ip_addresses()
{
    echo "IP Address Types"

    create_table "$1" "IPv4"
    $CH_CLIENT -q  "insert into test values (1, '116.253.40.133');"
    run_select "IPv4"
    drop_table

    create_table "$1" "IPv6"
    $CH_CLIENT -q  "insert into test values (1, '2001:44c8:129:2632:33:0:252:2');"
    run_select "IPv6"
    drop_table
}

function test_geo_types()
{
    echo "Geographical Types"

    create_table "$1" "Point"
    $CH_CLIENT -q  "insert into test values (1, (45, 89));"
    run_select "Point"
    drop_table

    create_table "$1" "Ring"
    $CH_CLIENT -q  "insert into test values (1, [(0, 0), (10, 0), (10, 10), (0, 10)]);"
    run_select "Ring"
    drop_table

    create_table "$1" "Polygon"
    $CH_CLIENT -q  "insert into test values (1, [[(20, 20), (50, 20), (50, 50), (20, 50)], [(30, 30), (50, 50), (50, 30)]]);"
    run_select "Polygon"
    drop_table

    create_table "$1" "MultiPolygon"
    $CH_CLIENT -q  "insert into test values (1, [[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (50, 20), (50, 50), (20, 50)],[(30, 30), (50, 50), (50, 30)]]]);"
    run_select "MultiPolygon"
    drop_table
}

drop_table

engines=("Memory" "MergeTree order by id settings min_rows_for_wide_part=100000000, min_bytes_for_wide_part=1000000000" "MergeTree order by id settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1")

for engine in "${engines[@]}"; do

   echo "$engine"

   test_integer_types "$engine"
   test_floating_point_numbers "$engine"
   test_boolean "$engine"
   test_strings "$engine"
   test_dates "$engine"
   test_json "$engine"
   test_uuid "$engine"
   test_low_cardinality_types "$engine"
   test_arrays "$engine"
   test_maps "$engine"
   test_nested_types "$engine"
   test_tuples "$engine"
   test_ip_addresses "$engine"
   test_geo_types "$engine"
done


