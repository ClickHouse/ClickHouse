#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

#${CLICKHOUSE_CLIENT} --max_block_size=1 --query="SELECT * FROM system.numbers LIMIT 10 FORMAT Parquet" > ${CLICKHOUSE_TMP}/t1.pq
#${CLICKHOUSE_CLIENT} --max_block_size=5 --query="SELECT * FROM system.numbers LIMIT 10 FORMAT Parquet" > ${CLICKHOUSE_TMP}/t5.pq
#${CLICKHOUSE_CLIENT} --max_block_size=15 --query="SELECT * FROM system.numbers LIMIT 10 FORMAT Parquet" > ${CLICKHOUSE_TMP}/t15.pq
#${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.numbers LIMIT 100000 FORMAT Parquet" > ${CLICKHOUSE_TMP}/t100000.pq
#${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.numbers LIMIT 1000000000 FORMAT Parquet" > ${CLICKHOUSE_TMP}/t1g.pq
#${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.numbers LIMIT 100000000 FORMAT Parquet" > ${CLICKHOUSE_TMP}/t100m.pq
#${CLICKHOUSE_CLIENT} --max_block_size=100000000 --query="SELECT * FROM system.numbers LIMIT 100000000 FORMAT Parquet" > ${CLICKHOUSE_TMP}/t100m-100mbs.pq
#valgrind --tool=massif  ${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.numbers LIMIT 1000000 FORMAT Parquet" > ${CLICKHOUSE_TMP}/t1g.pq


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS contributors"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE contributors (name String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.contributors ORDER BY name DESC FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO contributors FORMAT Parquet"
# random results
${CLICKHOUSE_CLIENT} --query="SELECT * FROM contributors LIMIT 10" > /dev/null
${CLICKHOUSE_CLIENT} --query="DROP TABLE contributors"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS parquet_numbers"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE parquet_numbers (number UInt64) ENGINE = Memory"
# less than default block size (65k)
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.numbers LIMIT 10000 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_numbers FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_numbers ORDER BY number DESC LIMIT 10"
${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE parquet_numbers"

# More than default block size
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.numbers LIMIT 100000 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_numbers FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_numbers ORDER BY number DESC LIMIT 10"
${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE parquet_numbers"

#${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.numbers LIMIT 10000000 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_numbers FORMAT Parquet"
#${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_numbers ORDER BY number DESC LIMIT 10"
#${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE parquet_numbers"

#${CLICKHOUSE_CLIENT} --max_block_size=2 --query="SELECT * FROM system.numbers LIMIT 3 FORMAT Parquet" > ${CLICKHOUSE_TMP}/bs2.pq
${CLICKHOUSE_CLIENT} --max_block_size=2 --query="SELECT * FROM system.numbers LIMIT 3 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_numbers FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_numbers ORDER BY number DESC LIMIT 10"

${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE parquet_numbers"
${CLICKHOUSE_CLIENT} --max_block_size=1 --query="SELECT * FROM system.numbers LIMIT 1000 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_numbers FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_numbers ORDER BY number DESC LIMIT 10"

${CLICKHOUSE_CLIENT} --query="DROP TABLE parquet_numbers"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS parquet_events"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE parquet_events (event String, value UInt64, description String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.events FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_events FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT event, description FROM parquet_events WHERE event IN ('ContextLock', 'Query') ORDER BY event"
${CLICKHOUSE_CLIENT} --query="DROP TABLE parquet_events"


#${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS parquet_types1"
#${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS parquet_types2"
#${CLICKHOUSE_CLIENT} --query="CREATE TABLE parquet_types1       (int8 Int8, uint8 UInt8, int16 Int16, uint16 UInt16, int32 Int32,                int64 Int64, uint64 UInt64, float32 Float32, float64 Float64, string String                                                           ) ENGINE = Memory"
#${CLICKHOUSE_CLIENT} --query="CREATE TABLE parquet_types2       (int8 Int8, uint8 UInt8, int16 Int16, uint16 UInt16, int32 Int32,                int64 Int64, uint64 UInt64, float32 Float32, float64 Float64, string String                                                           ) ENGINE = Memory"
#${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types1 values (     -108,         108,       -1016,          1116,       -1032,                      -1064,          1164,          -1.032,          -1.064,      'string'                                                    )"
#${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types1 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types2 FORMAT Parquet"



${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS parquet_types1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS parquet_types2"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS parquet_types3"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS parquet_types4"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE parquet_types1       (int8 Int8, uint8 UInt8, int16 Int16, uint16 UInt16, int32 Int32, uint32 UInt32, int64 Int64, uint64 UInt64, float32 Float32, float64 Float64, string String, fixedstring FixedString(15), date Date, datetime DateTime) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE parquet_types2       (int8 Int8, uint8 UInt8, int16 Int16, uint16 UInt16, int32 Int32, uint32 UInt32, int64 Int64, uint64 UInt64, float32 Float32, float64 Float64, string String, fixedstring FixedString(15), date Date, datetime DateTime) ENGINE = Memory"
# convert min type
${CLICKHOUSE_CLIENT} --query="CREATE TABLE parquet_types3       (int8 Int8,  uint8 Int8,  int16 Int8,   uint16 Int8,  int32 Int8,   uint32 Int8,  int64 Int8,   uint64 Int8,    float32 Int8,    float64 Int8, string FixedString(15), fixedstring FixedString(15), date Date,    datetime Date) ENGINE = Memory"
# convert max type
${CLICKHOUSE_CLIENT} --query="CREATE TABLE parquet_types4       (int8 Int64, uint8 Int64, int16 Int64, uint16 Int64, int32 Int64,  uint32 Int64, int64 Int64,  uint64 Int64,   float32 Int64,   float64 Int64, string String,          fixedstring String, date DateTime, datetime DateTime) ENGINE = Memory"

${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types1 values (     -108,         108,       -1016,          1116,       -1032,          1132,       -1064,          1164,          -1.032,          -1.064,    'string-0',               'fixedstring', '2001-02-03', '2002-02-03 04:05:06')"

# min
${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types1 values (     -128,           0,      -32768,             0, -2147483648,             0, -9223372036854775808, 0,             -1.032,          -1.064,    'string-1',             'fixedstring-1', '2003-04-05', '2003-02-03 04:05:06')"

# max
${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types1 values (      127,         255,       32767,         65535,  2147483647,    4294967295, 9223372036854775807, 9223372036854775807, -1.032,     -1.064,    'string-2',             'fixedstring-2', '2004-06-07', '2004-02-03 04:05:06')"

# 'SELECT -127,-128,-129,126,127,128,255,256,257,-32767,-32768,-32769,32766,32767,32768,65535,65536,65537,  -2147483647,-2147483648,-2147483649,2147483646,2147483647,2147483648,4294967295,4294967296,4294967297,   -9223372036854775807,-9223372036854775808,9223372036854775806,9223372036854775807,9223372036854775808,18446744073709551615';

${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types1 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types2 FORMAT Parquet"

echo original:
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types1 ORDER BY int8" | tee "${CLICKHOUSE_TMP}"/parquet_all_types_1.dump
echo converted:
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types2 ORDER BY int8" | tee "${CLICKHOUSE_TMP}"/parquet_all_types_2.dump
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types1 ORDER BY int8 FORMAT Parquet" > "${CLICKHOUSE_TMP}"/parquet_all_types_1.parquet
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types2 ORDER BY int8 FORMAT Parquet" > "${CLICKHOUSE_TMP}"/parquet_all_types_2.parquet
echo diff:
diff "${CLICKHOUSE_TMP}"/parquet_all_types_1.dump "${CLICKHOUSE_TMP}"/parquet_all_types_2.dump

${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE parquet_types2"
${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types3 values (       79,          81,          82,            83,          84,            85,          86,            87,              88,              89,         'str01',                  'fstr1', '2003-03-04', '2004-05-06')"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types3 ORDER BY int8 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types2 FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types1 ORDER BY int8 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types3 FORMAT Parquet"

${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types4 values (       80,          81,          82,            83,          84,            85,          86,            87,              88,              89,         'str02',                  'fstr2', '2005-03-04 05:06:07', '2006-08-09 10:11:12')"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types4 ORDER BY int8 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types2 FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types1 ORDER BY int8 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types4 FORMAT Parquet"

echo dest:
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types2 ORDER BY int8"
echo min:
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types3 ORDER BY int8"
echo max:
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types4 ORDER BY int8"


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS parquet_types5"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS parquet_types6"
${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE parquet_types2"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE parquet_types5       (int8 Nullable(Int8), uint8 Nullable(UInt8), int16 Nullable(Int16), uint16 Nullable(UInt16), int32 Nullable(Int32), uint32 Nullable(UInt32), int64 Nullable(Int64), uint64 Nullable(UInt64), float32 Nullable(Float32), float64 Nullable(Float64), string Nullable(String), fixedstring Nullable(FixedString(15)), date Nullable(Date), datetime Nullable(DateTime)) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE parquet_types6       (int8 Nullable(Int8), uint8 Nullable(UInt8), int16 Nullable(Int16), uint16 Nullable(UInt16), int32 Nullable(Int32), uint32 Nullable(UInt32), int64 Nullable(Int64), uint64 Nullable(UInt64), float32 Nullable(Float32), float64 Nullable(Float64), string Nullable(String), fixedstring Nullable(FixedString(15)), date Nullable(Date), datetime Nullable(DateTime)) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types5 values (               NULL,                  NULL,                  NULL,                    NULL,                  NULL,                    NULL,                  NULL,                    NULL,                      NULL,                      NULL,                    NULL,                                  NULL,                NULL,                        NULL)"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types5 ORDER BY int8 FORMAT Parquet" > "${CLICKHOUSE_TMP}"/parquet_all_types_5.parquet
#${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types5 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types6 FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types5 ORDER BY int8 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types6 FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types1 ORDER BY int8 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types6 FORMAT Parquet"
echo dest from null:
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types6 ORDER BY int8"


${CLICKHOUSE_CLIENT} --query="DROP TABLE parquet_types5"
${CLICKHOUSE_CLIENT} --query="DROP TABLE parquet_types6"


${CLICKHOUSE_CLIENT} --query="DROP TABLE parquet_types1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE parquet_types2"
${CLICKHOUSE_CLIENT} --query="DROP TABLE parquet_types3"
${CLICKHOUSE_CLIENT} --query="DROP TABLE parquet_types4"


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS parquet_arrays"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE parquet_arrays (id UInt32, a1 Array(Int8), a2 Array(UInt8), a3 Array(Int16), a4 Array(UInt16), a5 Array(Int32), a6 Array(UInt32), a7 Array(Int64), a8 Array(UInt64), a9 Array(String), a10 Array(FixedString(4)), a11 Array(Float32), a12 Array(Float64), a13 Array(Date), a14 Array(Datetime), a15 Array(Decimal(4, 2)), a16 Array(Decimal(10, 2)), a17 Array(Decimal(25, 2))) engine=Memory()"

${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_arrays VALUES (1, [1,-2,3], [1,2,3], [100, -200, 300], [100, 200, 300], [10000000, -20000000, 30000000], [10000000, 2000000, 3000000], [100000000000000, -200000000000, 3000000000000], [100000000000000, 20000000000000, 3000000000000], ['Some string', 'Some string', 'Some string'], ['0000', '1111', '2222'], [42.42, 424.2, 0.4242], [424242.424242, 4242042420.242424, 42], ['2000-01-01', '2001-01-01', '2002-01-01'], ['2000-01-01', '2001-01-01', '2002-01-01'], [0.2, 10.003, 4.002], [4.000000001, 10000.10000, 10000.100001], [1000000000.000000001123, 90.0000000010010101, 0101001.0112341001])"

${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_arrays VALUES (2, [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [])"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_arrays FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_arrays FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_arrays ORDER BY id"

${CLICKHOUSE_CLIENT} --query="DROP TABLE parquet_arrays"


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS parquet_nullable_arrays"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE parquet_nullable_arrays (id UInt32, a1 Array(Nullable(UInt32)), a2 Array(Nullable(String)), a3 Array(Nullable(Decimal(4, 2)))) engine=Memory()"
${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_nullable_arrays VALUES (1, [1, Null, 2], [Null, 'Some string', Null], [0.001, Null, 42.42]), (2, [Null], [Null], [Null]), (3, [], [], [])"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_nullable_arrays FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_nullable_arrays FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_nullable_arrays ORDER BY id"
${CLICKHOUSE_CLIENT} --query="DROP TABLE parquet_nullable_arrays"


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS parquet_nested_arrays"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE parquet_nested_arrays (a1 Array(Array(Array(UInt32))), a2 Array(Array(Array(String))), a3 Array(Array(Nullable(UInt32))), a4 Array(Array(Nullable(String)))) engine=Memory() "
${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_nested_arrays VALUES ([[[1,2,3], [1,2,3]], [[1,2,3]], [[], [1,2,3]]], [[['Some string', 'Some string'], []], [['Some string']], [[]]], [[Null, 1, 2], [Null], [1, 2], []], [['Some string', Null, 'Some string'], [Null], []])"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_nested_arrays FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_nested_arrays FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_nested_arrays"
${CLICKHOUSE_CLIENT} --query="DROP TABLE parquet_nested_arrays"


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS parquet_decimal"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE parquet_decimal (d1 Decimal32(4), d2 Decimal64(8), d3 Decimal128(16), d4 Decimal256(32)) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE parquet_decimal VALUES (0.123, 0.123123123, 0.123123123123, 0.123123123123123123)"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_decimal FORMAT Arrow" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_decimal FORMAT Arrow"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_decimal"
${CLICKHOUSE_CLIENT} --query="DROP TABLE parquet_decimal"
