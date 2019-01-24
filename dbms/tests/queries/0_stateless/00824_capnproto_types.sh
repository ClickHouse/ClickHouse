#!/usr/bin/env bash

set -e 

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

#create the schema file
echo "
@0xdaaa5235225a9b9b;
struct NativeTypeTest {
     void @0 :Void;
     bool @1 :Bool;
     int8 @2 :Int8;
     int16 @3 :Int16;
     int32 @4 :Int32;
     int64 @5 :Int64;
     uint8 @6 :UInt8;
     uint16 @7 :UInt16;
     uint32 @8 :UInt32;
     uint64 @9 :UInt64;
     float32 @10 :Float32;
     float64 @11 :Float64;
     text @12 :Text;
     data @13 :Data;
     list @14 :List(Bool);
}" > test.capnp

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test.capnproto_types"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test.capnproto_types
(
    bool Int8,
    int8 Int8,
    int16 Int16,
    int32 Int32,
    int64 Int64,
    uint8 UInt8,
    uint16 UInt16,
    uint32 UInt32,
    uint64 UInt64,
    float32 Float32,
    float64 Float64,
    text String,
    data String,
    list Array(Int8)
) ENGINE = Memory"

echo -ne '\x00\x00\x00\x00\x0d\x00\x00\x00\x00\x00\x00\x00\x06\x00\x03\x00\x01\x02\x03\x00\x04\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x06\x00\x07\x00\x08\x00\x00\x00\x09\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x41\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x26\x40\x09\x00\x00\x00\x1a\x00\x00\x00\x09\x00\x00\x00\x12\x00\x00\x00\x09\x00\x00\x00\x21\x00\x00\x00\x31\x32\x00\x00\x00\x00\x00\x00\x1f\x21\x00\x00\x00\x00\x00\x00\x09\x00\x00\x00\x00\x00\x00\x00' | $CLICKHOUSE_CLIENT --stacktrace --format_schema='test:NativeTypeTest' --query="INSERT INTO test.capnproto_types FORMAT CapnProto";

$CLICKHOUSE_CLIENT -q "SELECT * FROM test.capnproto_types"
$CLICKHOUSE_CLIENT -q "DROP TABLE test.capnproto_types"

# remove the schema file
rm test.capnp

