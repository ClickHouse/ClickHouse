#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CUR_DIR/../shell_config.sh

DATA_DIR=$CUR_DIR/data_avro

# input
echo === input
echo = primitive

cat $DATA_DIR/primitive.avro | ${CLICKHOUSE_LOCAL}  --input-format Avro --output-format CSV -S 'a_bool UInt8, b_int Int32, c_long Int64, d_float Float32, e_double Float64, f_bytes String, g_string String' -q 'select * from table'
cat $DATA_DIR/primitive.avro | ${CLICKHOUSE_LOCAL}  --input-format Avro --output-format CSV -S 'a_bool UInt8, c_long Int64, g_string String' -q 'select * from table'
cat $DATA_DIR/primitive.avro | ${CLICKHOUSE_LOCAL}  --input-format Avro --output-format CSV -S 'g_string String, c_long Int64, a_bool UInt8' -q 'select * from table'
cat $DATA_DIR/primitive.avro | ${CLICKHOUSE_LOCAL}  --input-format Avro --output-format CSV -S 'g_string String' -q 'select * from table'

echo = complex
cat $DATA_DIR/complex.avro | ${CLICKHOUSE_LOCAL}  --input-format Avro --output-format CSV -S "a_enum_to_string String, b_enum_to_enum Enum('t' = 1, 'f' = 0), c_array_string Array(String), d_array_array_string Array(Array(String)), e_union_null_string Nullable(String), f_union_long_null Nullable(Int64), g_fixed FixedString(32)" -q 'select * from table'
cat $DATA_DIR/complex.avro | ${CLICKHOUSE_LOCAL}  --input-format Avro --output-format CSV -S "g_fixed FixedString(32)" -q 'select * from table'

echo = logical_types
cat $DATA_DIR/logical_types.avro | ${CLICKHOUSE_LOCAL}  --input-format Avro --output-format CSV -S 'a_date Date, b_timestamp_millis DateTime64, c_timestamp_micros DateTime64(6)' -q 'select * from table'
cat $DATA_DIR/logical_types.avro | ${CLICKHOUSE_LOCAL}  --input-format Avro --output-format CSV -S 'a_date Int32, b_timestamp_millis Int64, c_timestamp_micros Int64' -q 'select * from table'



echo = compression
cat $DATA_DIR/simple.null.avro | ${CLICKHOUSE_LOCAL}  --input-format Avro --output-format CSV -S 'a Int64' -q 'select count() from table'
cat $DATA_DIR/simple.deflate.avro | ${CLICKHOUSE_LOCAL}  --input-format Avro --output-format CSV -S 'a Int64' -q 'select count() from table'
cat $DATA_DIR/simple.snappy.avro | ${CLICKHOUSE_LOCAL}  --input-format Avro --output-format CSV -S 'a Int64' -q 'select count() from table'

echo = other
#no data
cat $DATA_DIR/empty.avro | ${CLICKHOUSE_LOCAL}  --input-format Avro --output-format CSV -S 'a Int64' -q 'select count() from table'
# type mismatch
cat $DATA_DIR/simple.null.avro | ${CLICKHOUSE_LOCAL}  --input-format Avro --output-format CSV -S 'a Int32' -q 'select count() from table' 2>&1 | grep -i 'not compatible' -o
# field not found
cat $DATA_DIR/simple.null.avro | ${CLICKHOUSE_LOCAL}  --input-format Avro --output-format CSV -S 'b Int64' -q 'select count() from table' 2>&1 | grep -i 'not found' -o






# output
echo === output

echo = primitive
S1="a_bool UInt8, b_int Int32, c_long Int64, d_float Float32, e_double Float64, f_bytes String, g_string String"
echo '1,1,2,3.4,5.6,"b1","s1"' | ${CLICKHOUSE_LOCAL} --input-format CSV -S "$S1" -q "select * from table  format Avro" | ${CLICKHOUSE_LOCAL}  --input-format Avro --output-format CSV -S "$S1" -q 'select * from table'

echo = complex
S2="a_enum_to_string String, b_enum_to_enum Enum('t' = 1, 'f' = 0), c_array_string Array(String), d_array_array_string Array(Array(String)), e_union_null_string Nullable(String), f_union_long_null Nullable(Int64), g_fixed FixedString(32)"
echo "\"A\",\"t\",\"['s1','s2']\",\"[['a1'],['a2']]\",\"s1\",\N,\"79cd909892d7e7ade1987cc7422628ba\"" | ${CLICKHOUSE_LOCAL} --input-format CSV -S "$S2" -q "select * from table  format Avro" | ${CLICKHOUSE_LOCAL}  --input-format Avro --output-format CSV -S "$S2" -q 'select * from table'

echo = logical_types
S3="a_date Date, b_timestamp_millis DateTime64, c_timestamp_micros DateTime64(6)"
echo '"2019-12-20","2020-01-10 01:31:56.227","2020-01-10 01:31:56.227000"' | ${CLICKHOUSE_LOCAL} --input-format CSV -S "$S3" -q "select * from table  format Avro" | ${CLICKHOUSE_LOCAL}  --input-format Avro --output-format CSV -S "$S3" -q 'select * from table'

echo = other
S4="a Int64"
${CLICKHOUSE_LOCAL} -q "select toInt64(number) as a from numbers(0)  format Avro" | ${CLICKHOUSE_LOCAL}  --input-format Avro --output-format CSV -S "$S4" -q 'select count() from table'
${CLICKHOUSE_LOCAL} -q "select toInt64(number) as a from numbers(1000)  format Avro" | ${CLICKHOUSE_LOCAL}  --input-format Avro --output-format CSV -S "$S4" -q 'select count() from table'

# type not supported
${CLICKHOUSE_LOCAL}  -q "select toInt16(123) as a format Avro" 2>&1 | grep -i 'not supported' -o