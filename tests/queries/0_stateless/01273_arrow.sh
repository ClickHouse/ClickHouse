#!/usr/bin/env bash
# Tags: no-unbundled, no-fasttest

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS contributors"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE contributors (name String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.contributors ORDER BY name DESC FORMAT Arrow" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO contributors FORMAT Arrow"
# random results
${CLICKHOUSE_CLIENT} --query="SELECT * FROM contributors LIMIT 10" > /dev/null
${CLICKHOUSE_CLIENT} --query="DROP TABLE contributors"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_numbers"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_numbers (number UInt64) ENGINE = Memory"
# less than default block size (65k)
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.numbers LIMIT 10000 FORMAT Arrow" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_numbers FORMAT Arrow"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_numbers ORDER BY number DESC LIMIT 10"
${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE arrow_numbers"

# More than default block size
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.numbers LIMIT 100000 FORMAT Arrow" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_numbers FORMAT Arrow"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_numbers ORDER BY number DESC LIMIT 10"
${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE arrow_numbers"

${CLICKHOUSE_CLIENT} --max_block_size=2 --query="SELECT * FROM system.numbers LIMIT 3 FORMAT Arrow" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_numbers FORMAT Arrow"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_numbers ORDER BY number DESC LIMIT 10"

${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE arrow_numbers"
${CLICKHOUSE_CLIENT} --max_block_size=1 --query="SELECT * FROM system.numbers LIMIT 1000 FORMAT Arrow" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_numbers FORMAT Arrow"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_numbers ORDER BY number DESC LIMIT 10"

${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_numbers"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_types1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_types2"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_types3"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_types4"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_types1       (int8 Int8, uint8 UInt8, int16 Int16, uint16 UInt16, int32 Int32, uint32 UInt32, int64 Int64, uint64 UInt64, float32 Float32, float64 Float64, string String, fixedstring FixedString(15), date Date, datetime DateTime) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_types2       (int8 Int8, uint8 UInt8, int16 Int16, uint16 UInt16, int32 Int32, uint32 UInt32, int64 Int64, uint64 UInt64, float32 Float32, float64 Float64, string String, fixedstring FixedString(15), date Date, datetime DateTime) ENGINE = Memory"
# convert min type
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_types3       (int8 Int8,  uint8 Int8,  int16 Int8,   uint16 Int8,  int32 Int8,   uint32 Int8,  int64 Int8,   uint64 Int8,    float32 Int8,    float64 Int8, string FixedString(15), fixedstring FixedString(15), date Date,    datetime Date) ENGINE = Memory"
# convert max type
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_types4       (int8 Int64, uint8 Int64, int16 Int64, uint16 Int64, int32 Int64,  uint32 Int64, int64 Int64,  uint64 Int64,   float32 Int64,   float64 Int64, string String,          fixedstring String, date DateTime, datetime DateTime) ENGINE = Memory"

${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_types1 values (     -108,         108,       -1016,          1116,       -1032,          1132,       -1064,          1164,          -1.032,          -1.064,    'string-0',               'fixedstring', '2001-02-03', '2002-02-03 04:05:06')"

# min
${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_types1 values (     -128,           0,      -32768,             0, -2147483648,             0, -9223372036854775808, 0,             -1.032,          -1.064,    'string-1',             'fixedstring-1', '2003-04-05', '2003-02-03 04:05:06')"

# max
${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_types1 values (      127,         255,       32767,         65535,  2147483647,    4294967295, 9223372036854775807, 9223372036854775807, -1.032,     -1.064,    'string-2',             'fixedstring-2', '2004-06-07', '2004-02-03 04:05:06')"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_types1 FORMAT Arrow" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_types2 FORMAT Arrow"

echo original:
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_types1 ORDER BY int8" | tee "${CLICKHOUSE_TMP}"/arrow_all_types_1.dump
echo converted:
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_types2 ORDER BY int8" | tee "${CLICKHOUSE_TMP}"/arrow_all_types_2.dump
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_types1 ORDER BY int8 FORMAT Arrow" > "${CLICKHOUSE_TMP}"/arrow_all_types_1.arrow
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_types2 ORDER BY int8 FORMAT Arrow" > "${CLICKHOUSE_TMP}"/arrow_all_types_2.arrow
echo diff:
diff "${CLICKHOUSE_TMP}"/arrow_all_types_1.dump "${CLICKHOUSE_TMP}"/arrow_all_types_2.dump

${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE arrow_types2"
${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_types3 values (       79,          81,          82,            83,          84,            85,          86,            87,              88,              89,         'str01',                  'fstr1', '2003-03-04', '2004-05-06')"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_types3 ORDER BY int8 FORMAT Arrow" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_types2 FORMAT Arrow"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_types1 ORDER BY int8 FORMAT Arrow" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_types3 FORMAT Arrow"

${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_types4 values (       80,          81,          82,            83,          84,            85,          86,            87,              88,              89,         'str02',                  'fstr2', '2005-03-04 05:06:07', '2006-08-09 10:11:12')"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_types4 ORDER BY int8 FORMAT Arrow" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_types2 FORMAT Arrow"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_types1 ORDER BY int8 FORMAT Arrow" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_types4 FORMAT Arrow"

echo dest:
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_types2 ORDER BY int8"
echo min:
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_types3 ORDER BY int8"
echo max:
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_types4 ORDER BY int8"


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_types5"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_types6"
${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE arrow_types2"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_types5       (int8 Nullable(Int8), uint8 Nullable(UInt8), int16 Nullable(Int16), uint16 Nullable(UInt16), int32 Nullable(Int32), uint32 Nullable(UInt32), int64 Nullable(Int64), uint64 Nullable(UInt64), float32 Nullable(Float32), float64 Nullable(Float64), string Nullable(String), fixedstring Nullable(FixedString(15)), date Nullable(Date), datetime Nullable(DateTime)) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_types6       (int8 Nullable(Int8), uint8 Nullable(UInt8), int16 Nullable(Int16), uint16 Nullable(UInt16), int32 Nullable(Int32), uint32 Nullable(UInt32), int64 Nullable(Int64), uint64 Nullable(UInt64), float32 Nullable(Float32), float64 Nullable(Float64), string Nullable(String), fixedstring Nullable(FixedString(15)), date Nullable(Date), datetime Nullable(DateTime)) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_types5 values (               NULL,                  NULL,                  NULL,                    NULL,                  NULL,                    NULL,                  NULL,                    NULL,                      NULL,                      NULL,                    NULL,                                  NULL,                NULL,                        NULL)"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_types5 ORDER BY int8 FORMAT Arrow" > "${CLICKHOUSE_TMP}"/arrow_all_types_5.arrow
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_types5 ORDER BY int8 FORMAT Arrow" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_types6 FORMAT Arrow"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_types1 ORDER BY int8 FORMAT Arrow" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_types6 FORMAT Arrow"
echo dest from null:
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_types6 ORDER BY int8"

${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_types5"
${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_types6"


${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_types1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_types2"
${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_types3"
${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_types4"

