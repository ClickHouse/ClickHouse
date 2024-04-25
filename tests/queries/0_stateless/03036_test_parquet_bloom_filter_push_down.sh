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
# import pyarrow as pa
# from pyarrow import parquet as pq
# import numpy as np
# np.random.seed(0)
# table = pa.Table.from_pydict({
#   'f32': np.random.normal(size=100000).astype(np.float32),
#   'f64': np.random.normal(size=100000).astype(np.float64),
# })
#
# pq.write_table(
#   table,
#   'simple-fd.parquet'
# )
# #-
#
# import pyspark
# from pyspark.sql import SparkSession
# spark=SparkSession.builder.appName("parquetFile").config("spark.driver.memory", "45g").getOrCreate()
#
# house_df = spark.read.parquet('simple-fd.parquet')
#
# house_df.coalesce(1).write.mode('overwrite').option('parquet.bloom.filter.enabled#f32', "true").option('parquet.bloom.filter.enabled#f64', "true").option("parquet.block.size", 1024*1024).parquet("simple-fd-bf.parquet")

fd_filename="simple-fd-bf.parquet"

${CLICKHOUSE_CLIENT} --query="select count(*) from file('${fd_filename}', Parquet) SETTINGS use_cache_for_count_from_files=false;"
# bloom filter is off, all row groups should be read
# expect rows_read = select count()
${CLICKHOUSE_CLIENT} --query="select * from file('${fd_filename}', Parquet) where f32=toFloat32(1.3040012) or f64=toFloat64(0.631960187601809) Format JSON SETTINGS input_format_parquet_bloom_filter_push_down=false, input_format_parquet_filter_push_down=false;" | jq 'del(.meta,.statistics.elapsed)'
# bloom filter is on, some row groups should be skipped
# expect rows_read much less than select count()
${CLICKHOUSE_CLIENT} --query="select * from file('${fd_filename}', Parquet) where f32=toFloat32(1.3040012) or f64=toFloat64(0.631960187601809) Format JSON SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;" | jq 'del(.meta,.statistics.elapsed)'
# bloom filter is on and it is filtering by data in the other row group, which is bigger than the former. Expect most row groups to be read, but not all
# expect rows_read less than select count(), but higher than previous query
${CLICKHOUSE_CLIENT} --query="select * from file('${fd_filename}', Parquet) where f32 = toFloat32(1.7640524) and f64 = toFloat64(-0.48379749195754734) Format JSON SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;" | jq 'del(.meta,.statistics.elapsed)'
# bloom filter is on, but includes data from both (all row groups), all row groups should be read
# expect rows_read = select count()
${CLICKHOUSE_CLIENT} --query="select * from file('${fd_filename}', Parquet) where f32=toFloat32(1.3040012) or f64=toFloat64(0.631960187601809) or f32 = toFloat32(1.7640524) and f64 = toFloat64(-0.48379749195754734) Format JSON SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;" | jq 'del(.meta,.statistics.elapsed)'

