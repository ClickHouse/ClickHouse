#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')


$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS msgpack";

$CLICKHOUSE_CLIENT --query="CREATE TABLE msgpack (uint8 UInt8, uint16 UInt16, uint32 UInt32, uint64 UInt64, int8 Int8, int16 Int16, int32 Int32, int64 Int64, float Float32, double Float64, string String, date Date, datetime DateTime('Europe/Moscow'), datetime64 DateTime64(3, 'Europe/Moscow'), array Array(UInt32)) ENGINE = Memory";


$CLICKHOUSE_CLIENT --query="INSERT INTO msgpack VALUES (255, 65535, 4294967295, 100000000000, -128, -32768, -2147483648, -100000000000, 2.02, 10000.0000001, 'String', 18980, 1639872000, 1639872000000, [1,2,3,4,5]), (4, 1234, 3244467295, 500000000000, -1, -256, -14741221, -7000000000, 100.1, 14321.032141201, 'Another string', 20000, 1839882000, 1639872891123, [5,4,3,2,1]), (42, 42, 42, 42, 42, 42, 42, 42, 42.42, 42.42, '42', 42, 42, 42, [42])";

$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack FORMAT MsgPack" > "$CURDIR"/tmp_msgpac_test_all_types.msgpk;

cat "$CURDIR"/tmp_msgpac_test_all_types.msgpk | $CLICKHOUSE_CLIENT --query="INSERT INTO msgpack FORMAT MsgPack";

rm  "$CURDIR"/tmp_msgpac_test_all_types.msgpk

$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack";

$CLICKHOUSE_CLIENT --query="DROP TABLE msgpack";


$CLICKHOUSE_CLIENT --query="CREATE TABLE msgpack (array1 Array(Array(UInt32)), array2 Array(Array(Array(String)))) ENGINE = Memory";

$CLICKHOUSE_CLIENT --query="INSERT INTO msgpack VALUES ([[1,2,3], [1001, 2002], [3167]], [[['one'], ['two']], [['three']],[['four'], ['five']]])";

$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack FORMAT MsgPack" > "$CURDIR"/tmp_msgpack_test_nested_arrays.msgpk;

cat "$CURDIR"/tmp_msgpack_test_nested_arrays.msgpk | $CLICKHOUSE_CLIENT --query="INSERT INTO msgpack FORMAT MsgPack";
rm "$CURDIR"/tmp_msgpack_test_nested_arrays.msgpk;

$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack";

$CLICKHOUSE_CLIENT --query="DROP TABLE msgpack";


$CLICKHOUSE_CLIENT --query="CREATE TABLE msgpack (array Array(UInt8)) ENGINE = Memory";

$CLICKHOUSE_CLIENT --query="INSERT INTO msgpack VALUES ([0, 1, 2, 3, 42, 253, 254, 255]), ([255, 254, 253, 42, 3, 2, 1, 0])";

$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack FORMAT MsgPack" > "$CURDIR"/tmp_msgpack_type_conversion.msgpk;

$CLICKHOUSE_CLIENT --query="DROP TABLE msgpack";

$CLICKHOUSE_CLIENT --query="CREATE TABLE msgpack (array Array(Int64)) ENGINE = Memory";

cat "$CURDIR"/tmp_msgpack_type_conversion.msgpk | $CLICKHOUSE_CLIENT --query="INSERT INTO msgpack FORMAT MsgPack";
rm "$CURDIR"/tmp_msgpack_type_conversion.msgpk;

$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack";

$CLICKHOUSE_CLIENT --query="DROP TABLE msgpack";

$CLICKHOUSE_CLIENT --query="CREATE TABLE msgpack (date FixedString(10)) ENGINE = Memory";

$CLICKHOUSE_CLIENT --query="INSERT INTO msgpack VALUES ('2020-01-01'), ('2020-01-02'), ('2020-01-02')";

$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack";

$CLICKHOUSE_CLIENT --query="DROP TABLE msgpack";


$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS msgpack_map";

$CLICKHOUSE_CLIENT --query="CREATE TABLE msgpack_map (m Map(UInt64, UInt64), a Array(Map(UInt64, Array(UInt64)))) ENGINE=Memory()";

$CLICKHOUSE_CLIENT --query="INSERT INTO msgpack_map VALUES ({1 : 2, 2 : 3}, [{1 : [1, 2], 2 : [3, 4]}, {3 : [5, 6], 4 : [7, 8]}])";


$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack_map FORMAT MsgPack" | $CLICKHOUSE_CLIENT --query="INSERT INTO msgpack_map FORMAT MsgPack";

$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack_map";

$CLICKHOUSE_CLIENT --query="DROP TABLE msgpack_map";


$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS msgpack_lc_nullable";

$CLICKHOUSE_CLIENT --query="CREATE TABLE msgpack_lc_nullable (a LowCardinality(String), b Nullable(String), c LowCardinality(Nullable(String)), d Array(Nullable(String)), e Array(LowCardinality(Nullable(String)))) engine=Memory()";

$CLICKHOUSE_CLIENT --query="INSERT INTO msgpack_lc_nullable VALUES ('42', '42', '42', ['42', '42'], ['42', '42']), ('42', NULL, NULL, [NULL, '42', NULL], [NULL, '42', NULL])";


$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack_lc_nullable FORMAT MsgPack" | $CLICKHOUSE_CLIENT --query="INSERT INTO msgpack_lc_nullable FORMAT MsgPack";

$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack_lc_nullable";

$CLICKHOUSE_CLIENT --query="DROP TABLE msgpack_lc_nullable";


$CLICKHOUSE_CLIENT --query="SELECT toString(number) FROM  numbers(10) FORMAT MsgPack" > $USER_FILES_PATH/data.msgpack

$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x UInt64')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x Float32')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x Array(UInt32)')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x Map(UInt64, UInt64)')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';


$CLICKHOUSE_CLIENT --query="SELECT number FROM  numbers(10) FORMAT MsgPack" > $USER_FILES_PATH/data.msgpack

$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x Float32')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x String')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x Array(UInt64)')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x Map(UInt64, UInt64)')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';


$CLICKHOUSE_CLIENT --query="SELECT [number, number + 1] FROM  numbers(10) FORMAT MsgPack" > $USER_FILES_PATH/data.msgpack

$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x Float32')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x String')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x UInt64')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x Map(UInt64, UInt64)')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';


$CLICKHOUSE_CLIENT --query="SELECT map(number, number + 1) FROM  numbers(10) FORMAT MsgPack" > $USER_FILES_PATH/data.msgpack

$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x Float32')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x String')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x UInt64')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x Array(UInt64)')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';


rm $USER_FILES_PATH/data.msgpack

