#!/usr/bin/env bash
# Tags: no-ubsan, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# File generated in two steps (why two steps? I don't remember):
#
# 1. create file with pyarrow
# 2. create bloom filters with pyspark
#
#import pyarrow as pa
#from pyarrow import parquet as pq
#import numpy as np
#import pyarrow.compute as pc
#import random
#import string
#
## Set the random seed for reproducibility
#np.random.seed(0)
#
#size = 3000
#
## Generating float columns
#f32_data = np.random.normal(size=size).astype(np.float32)
#f64_data = np.random.normal(size=size).astype(np.float64)
#
## Generating integer columns
#int_data = np.random.randint(low=0, high=10000, size=size, dtype='int32')
#
## Generating string columns
#str_data = np.array([''.join(random.choices(string.ascii_uppercase, k=np.random.randint(0, 5))) for i in range(size)])
#
#fixed_str_data = np.array([''.join(random.choices(string.ascii_uppercase, k=4)) for i in range(size)])
#
## Generating array columns (list of integers)
#array_data = pa.array([np.random.randint(0, 100, size=5).tolist() for _ in range(size)])
#
## Create a table with the generated data
#table = pa.Table.from_arrays(
#    [
#        pa.array(f32_data, pa.float32()),
#        pa.array(f64_data, pa.float64()),
#        pa.array(int_data, pa.int32()),
#        pa.array(str_data, pa.string()),
#        pa.array(fixed_str_data, pa.string()),
#        array_data
#    ],
#    names=[
#        'f32',
#        'f64',
#        'int',
#        'str',
#        'fixed_str',
#        'array'
#    ]
#)
#
## Write the table to a Parquet file
#pq.write_table(table, 'enhanced-fd.parquet')
# ----------------
#
#import pyspark
#from pyspark.sql import SparkSession
#spark=SparkSession.builder.appName("parquetFile").config("spark.driver.memory", "45g").getOrCreate()
#
#house_df = spark.read.parquet('enhanced-fd.parquet')
#
#house_df.coalesce(1).write.mode('overwrite').option('parquet.bloom.filter.enabled#f32', "true").option('parquet.bloom.filter.enabled#f64', "true").option('parquet.bloom.filter.enabled#str', "true").option('parquet.bloom.filter.enabled#fixed_str', "true").option('parquet.bloom.filter.enabled#item', "true").option("parquet.block.size", 1024*50).parquet("enhanced", compression='gzip')

multi_column_filename="multi_column_bf.parquet"

${CLICKHOUSE_CLIENT} --query="select count(*) from file('${multi_column_filename}', Parquet) SETTINGS use_cache_for_count_from_files=false;"

# bloom filter is off, all row groups should be read
# expect rows_read = select count()
${CLICKHOUSE_CLIENT} --query="select * from file('${multi_column_filename}', Parquet) where f32=toFloat32(1.7640524) and fixed_str='GFZI' or str='OTDY' order by f32 asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=false, input_format_parquet_filter_push_down=false;"  | jq 'del(.meta,.statistics.elapsed)'

# bloom filter is on, some row groups should be skipped
# expect rows_read much less than select count()
${CLICKHOUSE_CLIENT} --query="select * from file('${multi_column_filename}', Parquet) where f32=toFloat32(1.7640524) and fixed_str='GFZI' or str='OTDY' order by f32 asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;"  | jq 'del(.meta,.statistics.elapsed)'

# IN check
${CLICKHOUSE_CLIENT} --query="select * from file('${multi_column_filename}', Parquet) where f32=toFloat32(1.7640524) and fixed_str in ('GFZI', 'OTDY') order by f32 asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;"  | jq 'del(.meta,.statistics.elapsed)'

# IN check for floats
${CLICKHOUSE_CLIENT} --query="select * from file('${multi_column_filename}', Parquet) where f32 in (toFloat32(1.7640524), toFloat32(0.09082593)) or fixed_str = 'RIJV' order by f32 asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;"  | jq 'del(.meta,.statistics.elapsed)'

# bloom filter is on and it is filtering by data in the other row group, which is bigger than the former. Expect most row groups to be read, but not all
# expect rows_read less than select count(), but higher than previous query
${CLICKHOUSE_CLIENT} --query="select * from file('multi_column_bf.parquet', Parquet) where str='PFRI' or str='ES' order by f32 asc Format JSON SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;" | jq 'del(.meta,.statistics.elapsed)'

# bloom filter is on, but includes data from both (all row groups), all row groups should be read
# expect rows_read = select count()
${CLICKHOUSE_CLIENT} --query="select * from file('multi_column_bf.parquet', Parquet) where str='PFRI' or str='ES' or str='OTDY' or fixed_str='GFZI' order by f32 asc Format JSON SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;" | jq 'del(.meta,.statistics.elapsed)'
