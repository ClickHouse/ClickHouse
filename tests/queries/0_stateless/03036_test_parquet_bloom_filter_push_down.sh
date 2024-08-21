#!/usr/bin/env bash
# Tags: no-ubsan, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# File generated with below script
#from pyspark.sql import SparkSession
#from pyspark.sql.functions import udf
#from pyspark.sql.types import StructType, StructField, FloatType, DoubleType, StringType, IntegerType, ArrayType
#import random
#
#spark = SparkSession.builder \
#    .appName("High Cardinality Parquet File Creator with Bloom Filters") \
#    .config("spark.sql.parquet.enable.bloom.filter", "true") \
#    .config("spark.driver.memory", "2g") \
#    .getOrCreate()
#
#schema = StructType([
#    StructField("f32", FloatType(), True),
#    StructField("f64", DoubleType(), True),
#    StructField("int", IntegerType(), True),
#    StructField("str", StringType(), True),
#    StructField("fixed_str", StringType(), True),
#    StructField("array", ArrayType(IntegerType()), True)
#])
#
#random.seed(42)
#
#def generate_data(num_rows):
#    data = []
#    for _ in range(num_rows):
#        f32 = random.uniform(-100, 100)
#        f64 = random.uniform(-100, 100)
#        int_value = random.randint(1, 100)
#        str_value = ''.join(random.choices('ABCDEFGJKLYZ', k=5))
#        fixed_str = ''.join(random.choices('ABCDNOUVWXYZ', k=4))
#        array = [random.randint(1, 50) for _ in range(5)]
#        data.append((f32, f64, int_value, str_value, fixed_str, array))
#    return data
#
#data = generate_data(3000)
#
#df = spark.createDataFrame(data, schema)
#
## Define a new UDF to generate array with bigger range
#@udf(ArrayType(IntegerType()))
#def expand_array():
#    return [random.randint(1, 99999) for _ in range(5)]
#
## Apply the UDF to the 'array' column
#df = df.withColumn("array", expand_array())
#
#df.coalesce(1).write.mode('overwrite')\
#    .option("parquet.bloom.filter.enabled", "true")\
#    .option("parquet.block.size", 1024*70)\
#    .option("parquet.bloom.filter.enabled#int", "false")\
#    .parquet("output.parquet", compression="gzip")
#
#spark.stop()


USER_FILES_PATH=$($CLICKHOUSE_CLIENT_BINARY --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

WORKING_DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

mkdir -p "${WORKING_DIR}"

DATA_FILE="${CUR_DIR}/data_parquet/multi_column_bf.gz.parquet"

DATA_FILE_USER_PATH="${WORKING_DIR}/multi_column_bf.gz.parquet"

cp ${DATA_FILE} ${DATA_FILE_USER_PATH}

${CLICKHOUSE_CLIENT} --query="select count(*) from file('${DATA_FILE_USER_PATH}', Parquet) SETTINGS use_cache_for_count_from_files=false;"

echo "bloom filter is off, all row groups should be read"
echo "expect rows_read = select count()"
${CLICKHOUSE_CLIENT} --query="select f32, fixed_str, str from file('${DATA_FILE_USER_PATH}', Parquet) where f32=toFloat32(-64.12787) and fixed_str='BYYC' or str='KCGEY' order by f32 asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=false, input_format_parquet_filter_push_down=false;"  | jq 'del(.meta,.statistics.elapsed)'

echo "bloom filter is on, some row groups should be skipped"
echo "expect rows_read much less than select count()"
${CLICKHOUSE_CLIENT} --query="select f32, fixed_str, str from file('${DATA_FILE_USER_PATH}', Parquet) where f32=toFloat32(-64.12787) and fixed_str='BYYC' or str='KCGEY' order by f32 asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;"  | jq 'del(.meta,.statistics.elapsed)'

echo "bloom filter is on, but where predicate contains data from 2 row groups out of 3."
echo "Rows read should be less than select count, by greater than previous selects"
${CLICKHOUSE_CLIENT} --query="select f32 from file('${DATA_FILE_USER_PATH}', Parquet) where f32=toFloat32(-64.12787) or f32=toFloat32(-95.748695)  order by f32 asc Format JSON SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;" | jq 'del(.meta,.statistics.elapsed)'

echo "bloom filter is on, but where predicate contains data from all row groups"
echo "expect rows_read = select count()"
${CLICKHOUSE_CLIENT} --query="select f32 from file('${DATA_FILE_USER_PATH}', Parquet) where f32=toFloat32(-64.12787) or f32=toFloat32(-95.748695) or f32=toFloat32(-42.54674) order by f32 asc Format JSON SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;" | jq 'del(.meta,.statistics.elapsed)'

echo "IN check"
${CLICKHOUSE_CLIENT} --query="select f32, fixed_str from file('${DATA_FILE_USER_PATH}', Parquet) where f32=toFloat32(-15.910733) and fixed_str in ('BYYC', 'DCXV') order by f32 asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;"  | jq 'del(.meta,.statistics.elapsed)'

echo "IN check for floats"
${CLICKHOUSE_CLIENT} --query="select f64 from file('${DATA_FILE_USER_PATH}', Parquet) where f64 in (toFloat64(22.89182051713945), toFloat64(68.62704389505595)) order by f32 asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;"  | jq 'del(.meta,.statistics.elapsed)'

echo "tuple in case, bf is off."
${CLICKHOUSE_CLIENT} --query="select str, fixed_str from file('${DATA_FILE_USER_PATH}', Parquet) where (str, fixed_str) in ('LYLDL', 'BYYC') order by f32 asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=false, input_format_parquet_filter_push_down=false;"  | jq 'del(.meta,.statistics.elapsed)'

echo "tuple in case, bf is on."
${CLICKHOUSE_CLIENT} --query="select str, fixed_str from file('${DATA_FILE_USER_PATH}', Parquet) where (str, fixed_str) in ('LYLDL', 'BYYC') order by f32 asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;"  | jq 'del(.meta,.statistics.elapsed)'

echo "complex tuple in case, bf is off"
${CLICKHOUSE_CLIENT} --query="select str, fixed_str from file('${DATA_FILE_USER_PATH}', Parquet) where (str, fixed_str) in (('NON1', 'NON1'), ('LYLDL', 'BYYC'), ('NON2', 'NON2')) order by f32 asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=false, input_format_parquet_filter_push_down=false;"  | jq 'del(.meta,.statistics.elapsed)'

echo "complex tuple in case, bf is on"
${CLICKHOUSE_CLIENT} --query="select str, fixed_str from file('${DATA_FILE_USER_PATH}', Parquet) where (str, fixed_str) in (('NON1', 'NON1'), ('LYLDL', 'BYYC'), ('NON2', 'NON2')) order by f32 asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;"  | jq 'del(.meta,.statistics.elapsed)'

echo "complex tuple in case, bf is on. Non existent"
${CLICKHOUSE_CLIENT} --query="select str, fixed_str from file('${DATA_FILE_USER_PATH}', Parquet) where (str, fixed_str) in (('NON1', 'NON1'), ('NON2', 'NON2'), ('NON3', 'NON3')) order by f32 asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;"  | jq 'del(.meta,.statistics.elapsed)'

rm -rf ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}/*
