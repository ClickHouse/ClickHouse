#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS msgpack";
$CLICKHOUSE_CLIENT --query="CREATE TABLE msgpack (uint8 UInt8, uint16 UInt16, uint32 UInt32, uint64 UInt64, int8 Int8, int16 Int16, int32 Int32, int64 Int64, float Float32, double Float64, string String, date Date, datetime DateTime, datetime64 DateTime64, array Array(UInt32)) ENGINE = Memory";


$CLICKHOUSE_CLIENT --query="INSERT INTO msgpack VALUES (255, 65535, 4294967295, 100000000000, -128, -32768, -2147483648, -100000000000, 2.02, 10000.0000001, 'String', 18980, 1639872000, 1639872000000, [1,2,3,4,5]), (4, 1234, 3244467295, 500000000000, -1, -256, -14741221, -7000000000, 100.1, 14321.032141201, 'Another string', 20000, 1839882000, 1639872891123, [5,4,3,2,1]),(42, 42, 42, 42, 42, 42, 42, 42, 42.42, 42.42, '42', 42, 42, 42, [42])";

$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack FORMAT MsgPack" > $CURDIR/data_msgpack/all_types.msgpk;

cat $CURDIR/data_msgpack/all_types.msgpk | $CLICKHOUSE_CLIENT --query="INSERT INTO msgpack FORMAT MsgPack";

$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack";

$CLICKHOUSE_CLIENT --query="DROP TABLE msgpack";

$CLICKHOUSE_CLIENT --query="CREATE TABLE msgpack (array1 Array(Array(UInt32)), array2 Array(Array(Array(String)))) ENGINE = Memory";

$CLICKHOUSE_CLIENT --query="INSERT INTO msgpack VALUES ([[1,2,3], [1001, 2002], [3167]], [[['one'], ['two']], [['three']],[['four'], ['five']]])";

$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack FORMAT MsgPack" > $CURDIR/data_msgpack/nested_arrays.msgpk;

cat $CURDIR/data_msgpack/nested_arrays.msgpk | $CLICKHOUSE_CLIENT --query="INSERT INTO msgpack FORMAT MsgPack";

$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack";

$CLICKHOUSE_CLIENT --query="DROP TABLE msgpack";

