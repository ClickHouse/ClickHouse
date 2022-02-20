#!/usr/bin/env bash
# Tags: long, no-fasttest

# set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# Generate from Hive.
# 1. Create Hive table with input format text.
#    CREATE TABLE default.test_hive_types( f_tinyint tinyint, f_smallint smallint, f_int int, f_integer int, f_bigint bigint, f_float float, f_double double, f_decimal decimal(10,0), f_timestamp timestamp, f_date date, f_string string, f_varchar varchar(100), f_char char(100), f_bool boolean, f_array_int array<int>, f_array_string array<string>, f_array_float array<float>, f_map_int map<string, int>, f_map_string map<string, string>, f_map_float map<string, float>, f_struct struct<a:string, b:int, c:float>) PARTITIONED BY( day string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION 'hdfs://testcluster/data/hive/default.db/test_hive_types' 
# 2. Insert two rows into Hive table.
#    insert into default.test_hive_types partition(day='2022-02-20') select 1, 2, 3, 4, 5, 6.11, 7.22, 8.333, '2022-02-20 14:47:04', '2022-02-20', 'hello world', 'hello world', 'hello world', true, array(1,2,3), array('hello world', 'hello world'), array(float(1.1),float(1.2)), map('a', 100, 'b', 200, 'c', 300), map('a', 'aa', 'b', 'bb', 'c', 'cc'), map('a', float(111.1), 'b', float(222.2), 'c', float(333.3)), named_struct('a', 'aaa', 'b', 200, 'c', float(333.3))
#    insert into default.test_hive_types partition(day='2022-02-19') select 1, 2, 3, 4, 5, 6.11, 7.22, 8.333, '2022-02-19 14:47:04', '2022-02-19', 'hello world', 'hello world', 'hello world', true, array(1,2,3), array('hello world', 'hello world'), array(float(1.1),float(1.2)), map('a', 100, 'b', 200, 'c', 300), map('a', 'aa', 'b', 'bb', 'c', 'cc'), map('a', float(111.1), 'b', float(222.2), 'c', float(333.3)), named_struct('a', 'aaa', 'b', 200, 'c', float(333.3))  
# 3. Download file from HDFS.
SAMPLE_FILE="$CUR_DIR/02222_file.txt"


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_hive_types"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test_hive_types( f_tinyint Int8, f_smallint Int16, f_int Int32, f_integer Int32, f_bigint Int64, f_float Float32, f_double Float64, f_decimal Float64, f_timestamp DateTime, f_date Date, f_string String, f_varchar String, f_char String, f_bool Boolean, f_array_int Array(Int32), f_array_string Array(String), f_array_float Array(Float32), f_map_int Map(String, Int32), f_map_string Map(String, String), f_map_float Map(String, Float32), f_struct Tuple(a String, b Int32, c Float32)) ENGINE = Memory"
cat ${SAMPLE_FILE} | ${CLICKHOUSE_CLIENT} --query="insert into test_hive_types format HiveText"
${CLICKHOUSE_CLIENT} --query="select * from test_hive_types FORMAT TSVWithNamesAndTypes"
